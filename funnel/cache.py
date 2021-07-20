import enum
import json
import typing

import fsspec
import pydantic

from .metadata_db.schemas import Artifact
from .registry import registry
from .serializers import Serializer, pick_serializer


class DuplicateKeyEnum(str, enum.Enum):
    skip = 'skip'
    overwrite = 'overwrite'
    check_collision = 'check_collision'
    raise_error = 'raise_error'


@pydantic.dataclasses.dataclass
class CacheStore:
    """Implements caching functionality. Support backends (in-memory, local, s3fs, etc...) scheme registered with fsspec.

    Some backends may require other dependencies. For example to work with S3 cache store, s3fs is required.
    """

    path: str
    readonly: bool = False
    on_duplicate_key: DuplicateKeyEnum = 'skip'
    storage_options: typing.Dict = None

    def __post_init_post_parse__(self):
        self.storage_options = {} if self.storage_options is None else self.storage_options
        self.mapper = fsspec.get_mapper(self.path, **self.storage_options)
        self.fs = self.mapper.fs
        self.raw_path = self.fs._strip_protocol(self.path)
        self.protocol = self.fs.protocol

    def _construct_item_path(self, key) -> str:
        return f'{self.path}/{key}'

    def get(self, key: str, serializer: str, **load_kwargs) -> typing.Any:
        """Returns the value for the key if the key is in the cache store"""
        if self.protocol == 'memory':
            data = self.mapper[key]
            return json.loads(data)

        else:
            serializer = registry.serializers.get(serializer)()
            return serializer.load(self._construct_item_path(key), **load_kwargs)

    def __contains__(self, key: str) -> bool:
        return key in self.mapper

    def keys(self) -> typing.List[str]:
        return list(self.mapper.keys())

    def delete(self, key: str, **kwargs: typing.Dict) -> None:
        self.fs.delete(key, **kwargs)

    def put(
        self,
        key: str,
        value,
        serializer: str = 'auto',
        dump_kwargs: typing.Dict = {},
        custom_fields: typing.Dict = {},
    ) -> Artifact:
        """Records and serializes key with its corresponding value in the cache store.

        Parameters
        ----------
        key : str
        value :
        serializer : str
        dump_kwargs : dict
        custom_fields : dict

        Returns
        -------
        artifact : Artifact
            an `Artifact` object with corresping asset serialization information

        """
        if not self.readonly:
            method = getattr(self, f'_put_{self.on_duplicate_key.value}')
            serializer_name = pick_serializer(value) if serializer == 'auto' else serializer
            serializer = registry.serializers.get(serializer_name)()
            artifact = Artifact(
                key=key,
                serializer=serializer_name,
                dump_kwargs=dump_kwargs,
                custom_fields=custom_fields,
            )
            method(key, value, serializer, **dump_kwargs)
            return artifact

    def _put_skip(self, key, value, serializer: Serializer, **serializer_kwargs) -> None:
        if key not in self:
            self._put_overwrite(key, value, serializer, **serializer_kwargs)

    def _put_overwrite(self, key, value, serializer: Serializer, **serializer_kwargs) -> None:
        with self.fs.transaction:
            if self.protocol == 'memory':
                self.mapper[key] = json.dumps(value).encode('utf-8')
            else:
                serializer.dump(value, self._construct_item_path(key), **serializer_kwargs)
