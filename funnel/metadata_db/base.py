import abc
import typing

import pandas as pd
import pydantic

from ..cache import CacheStore


@pydantic.dataclasses.dataclass
class BaseMetadataStore(abc.ABC):
    """Records metadata information about how assets/artifacts are
    cached in the cache store.

    Notes
    -----
    This could be expanded to record provenance of cached assets/artifacts
    """

    cache_store: CacheStore
    readonly: bool

    @abc.abstractmethod
    def put(self, key, value, **dump_kwargs) -> None:
        ...

    @abc.abstractmethod
    def get(self, key, **load_kwargs) -> typing.Any:
        ...


@pydantic.dataclasses.dataclass
class MemoryMetadataStore(BaseMetadataStore):
    """Records metadata information in an in-memory pandas DataFrame."""

    readonly: bool = False
    index: str = 'key'
    required_columns: typing.ClassVar[typing.List[str]] = [
        'key',
        'serializer',
        'load_kwargs',
        'dump_kwargs',
    ]

    def __post_init_post_parse__(self):
        self.df = pd.DataFrame(columns=self.required_columns).set_index(self.index)

    def put(self, key: str, value, serializer: str = 'auto', **dump_kwargs) -> None:
        """Records and serializes key with its corresponding value in the metadata and cache store.

        Parameters
        ----------
        key : str
        value :
            Any serializable Python object
        serializer : str
        **dump_kwargs : dict
        """
        receipt = self.cache_store.put(key, value, serializer, **dump_kwargs)
        self.df.loc[key] = receipt.dict()

    def get(self, key: str, **load_kwargs) -> typing.Any:
        """Returns the value for the key if the key is in both the metadata and cache stores.

        Parameters
        ----------
        key : str
        load_kwargs : dict
        """
        x = self.df.loc[key]
        _load_kwargs = load_kwargs or x.load_kwargs
        return self.cache_store.get(key, x.serializer, **_load_kwargs)
