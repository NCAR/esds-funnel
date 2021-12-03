import datetime
import enum
import json
import tempfile
import typing

import fsspec
import pydantic

from .registry import registry
from .serializers import pick_serializer


class Artifact(pydantic.BaseModel):
    key: str
    serializer: str
    load_kwargs: typing.Optional[typing.Dict] = pydantic.Field(default_factory=dict)
    dump_kwargs: typing.Optional[typing.Dict] = pydantic.Field(default_factory=dict)
    created_at: typing.Optional[datetime.datetime] = pydantic.Field(
        default_factory=datetime.datetime.utcnow
    )
    _value: typing.Any = pydantic.PrivateAttr(default=None)

    class Config:
        validate_assignment = True


class DuplicateKeyEnum(str, enum.Enum):
    skip = 'skip'
    overwrite = 'overwrite'
    check_collision = 'check_collision'
    raise_error = 'raise_error'


@pydantic.dataclasses.dataclass
class CacheStore:
    """Implements caching functionality. Support backends (in-memory, local, s3fs, etc...) scheme registered with fsspec.

    Some backends may require other dependencies. For example to work with S3 cache store, s3fs is required.

    Parameters
    ----------
    path : str
        the path to the cache store
    storage_options : dict
        the storage options for the cache store
    readonly : bool
        if True, the cache store is readonly
    on_duplicate_key : DuplicateKeyEnum
        the behavior when a key is duplicated in the cache store
    """

    path: str = tempfile.gettempdir()
    readonly: bool = False
    on_duplicate_key: DuplicateKeyEnum = 'skip'
    storage_options: typing.Dict = None

    def __post_init_post_parse__(self):
        self.storage_options = {} if self.storage_options is None else self.storage_options
        self.mapper = fsspec.get_mapper(self.path, **self.storage_options)
        self.fs = self.mapper.fs
        self.raw_path = self.fs._strip_protocol(self.path)
        self.protocol = self.fs.protocol
        self._ensure_dir(self.raw_path)
        self._suffix = '.artifact.json'
        self._metadata_store_prefix = 'funnel_metadata_store'
        self._metadata_store_path = self._construct_item_path(self._metadata_store_prefix)
        self._ensure_dir(self._metadata_store_path)

    def _ensure_dir(self, key: str) -> None:
        if not self.fs.exists(key):
            self.fs.makedirs(key, exist_ok=True)

    def _construct_item_path(self, key) -> str:
        return f'{self.path}/{key}'

    def _artifact_meta_relative_path(self, key: str) -> str:
        return f'{self._metadata_store_prefix}/{key}{self._suffix}'

    def _artifact_meta_full_path(self, key: str) -> str:
        return f'{self._metadata_store_path}/{key}{self._suffix}'

    def __contains__(self, key: str) -> bool:
        """Returns True if the key is in the cache store."""
        return self._artifact_meta_relative_path(key) in self.mapper

    def keys(self) -> typing.List[str]:
        """Returns a list of keys in the cache store."""
        keys = self.fs.ls(self._metadata_store_path)
        return [
            key.split(f'{self._metadata_store_prefix}/')[-1].split(self._suffix)[0] for key in keys
        ]

    def delete(self, key: str, dry_run: bool = True) -> None:
        """Deletes the key and corresponding artifact from the cache store.

        Parameters
        ----------
        key : str
            Key to delete from the cache store.
        dry_run : bool
            If True, the key is not deleted from the cache store. This is useful for debugging.
        """
        keys = [key, self._artifact_meta_relative_path(key)]
        if not dry_run:
            self.mapper.delitems(keys)
        else:
            print(f'DRY RUN: would delete items with keys: {repr(keys)}')

    def __getitem__(self, key: str) -> typing.Any:
        """Returns the artifact corresponding to the key."""
        return self.get(key)

    def __setitem__(self, key: str, value: typing.Any) -> None:
        """Sets the key and corresponding artifact in the cache store."""
        self.put(key, value)

    def __delitem__(self, key: str) -> None:
        """Deletes the key and corresponding artifact from the cache store."""
        self.delete(key, dry_run=False)

    @pydantic.validate_arguments
    def get(
        self,
        key: str,
        serializer: str = None,
        load_kwargs: typing.Dict[typing.Any, typing.Any] = None,
    ) -> typing.Any:
        """Returns the value for the key if the key is in the cache store.

        Parameters
        ----------
        key : str
            Key to get from the cache store.
        serializer : str
            The name of the serializer you want to use. The built-in
            serializers are:

                - 'auto' (default): automatically choose the serializer based on the type of the value
                - 'xarray.netcdf': requires xarray and netCDF4
                - 'xarray.zarr': requires xarray and zarr

            You can also register your own serializer via the @funnel.registry.serializers.register decorator.
        load_kwargs : dict
            Additional keyword arguments to pass to the serializer when loading artifact from the cache store.

        Returns
        -------
        value :
            the value for the key if the key is in the cache store.

        Examples
        --------
        >>> from funnel import CacheStore
        >>> store = CacheStore("/tmp/my-cache")
        >>> store.keys()
        ['foo']
        >>> store.get("foo")
        [1, 2, 3]
        """

        metadata_file = self._artifact_meta_relative_path(key)
        message = f'{key} not found in cache store: {self._metadata_store_path}'
        if key not in self:
            raise KeyError(message)
        try:
            artifact = Artifact(**json.loads(self.mapper[metadata_file]))
        except Exception as exc:
            raise KeyError(
                f'Unable to load artifact sidecar file {metadata_file} for key: {key}'
            ) from exc

        try:
            serializer_name = serializer or artifact.serializer
            load_kwargs = load_kwargs or artifact.load_kwargs
            serializer = registry.serializers.get(serializer_name)()
            return serializer.load(self._construct_item_path(artifact.key), **load_kwargs)
        except Exception as exc:
            raise ValueError(f'Unable to load artifact {artifact.key} from cache store') from exc

    @pydantic.validate_arguments
    def put(
        self,
        key: str,
        value: typing.Any,
        serializer: str = 'auto',
        dump_kwargs: typing.Dict[typing.Any, typing.Any] = None,
        custom_fields: typing.Dict[typing.Any, typing.Any] = None,
    ) -> Artifact:
        """Records and serializes key with its corresponding value in the cache store.

        Parameters
        ----------
        key : str
            Key to put in the cache store.
        value : typing.Any
            Value to put in the cache store.
        serializer : str
            The name of the serializer you want to use. The built-in
            serializers are:

                - 'auto' (default): automatically choose the serializer based on the type of the value
                - 'xarray.netcdf': requires xarray and netCDF4
                - 'xarray.zarr': requires xarray and zarr

            You can also register your own serializer via the @funnel.registry.serializers.register decorator.
        dump_kwargs : dict
            Additional keyword arguments to pass to the serializer when dumping artifact to the cache store.
        custom_fields : dict
            A dict with types that serialize to json. These fields can be used for searching artifacts in the metadata store.

        Returns
        -------
        value : typing.Any
            Reference to the value that was put in the cache store.

        Examples
        --------
        >>> from funnel import CacheStore
        >>> store = CacheStore("/tmp/my-cache")
        >>> store.keys()
        []
        >>> store.put("foo", [1, 2, 3])
        >>> store.keys()
        ['foo']

        """
        dump_kwargs = dump_kwargs or {}
        custom_fields = custom_fields or {}
        if not self.readonly:
            method = getattr(self, f'_put_{self.on_duplicate_key.value}')
            serializer_name = pick_serializer(value) if serializer == 'auto' else serializer
            artifact = Artifact(
                key=key,
                serializer=serializer_name,
                dump_kwargs=dump_kwargs,
                custom_fields=custom_fields,
            )
            artifact._value = value
            method(artifact)
            with self.fs.open(self._artifact_meta_full_path(key), 'w') as fobj:
                fobj.write(artifact.json(indent=2))
            return artifact._value

    def _put_skip(self, artifact: Artifact) -> None:
        if self._artifact_meta_relative_path(artifact.key) not in self:
            self._put_overwrite(artifact)

    def _put_overwrite(self, artifact: Artifact) -> None:
        serializer = registry.serializers.get(artifact.serializer)()
        with self.fs.transaction:
            serializer.dump(
                artifact._value, self._construct_item_path(artifact.key), **artifact.dump_kwargs
            )
