import enum
import json
import typing

import fsspec
import pydantic

from .serializers import Serializer, pick_serializer, serializers


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

    def __getitem__(self, key: str, *args, **kwargs):
        return self.get(key, *args, **kwargs)

    def _construct_item_path(self, key):
        return f'{self.path}/{key}'

    def get(self, key: str, serializer: str, **serializer_kwargs):
        if self.protocol == 'memory':
            data = self.mapper[key]
            return json.loads(data)

        else:
            serializer = serializers.get(serializer)()
            return serializer.load(self._construct_item_path(key), **serializer_kwargs)

    def __contains__(self, key: str):
        return key in self.mapper

    def keys(self):
        return list(self.mapper.keys())

    def delete(self, key, **kwargs):
        self.fs.delete(key, **kwargs)

    def put(self, key, value, serializer: str = 'auto', **serializer_kwargs):
        if not self.readonly:
            method = getattr(self, f'_put_{self.on_duplicate_key.value}')
            serializer = pick_serializer(value) if serializer == 'auto' else serializer
            serializer = serializers.get(serializer)()
            return method(key, value, serializer, **serializer_kwargs)

    def _put_skip(self, key, value, serializer: Serializer, **serializer_kwargs):
        if key not in self:
            self._put_overwrite(key, value, serializer, **serializer_kwargs)

    def _put_overwrite(self, key, value, serializer: Serializer, **serializer_kwargs):
        with self.fs.transaction:
            if self.protocol == 'memory':
                self.mapper[key] = json.dumps(value).encode('utf-8')
            else:
                serializer.dump(value, self._construct_item_path(key), **serializer_kwargs)
