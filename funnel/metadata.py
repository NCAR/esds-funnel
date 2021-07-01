import abc
import typing

import pandas as pd
import pydantic


@pydantic.dataclasses.dataclass
class BaseMetadataStore(abc.ABC):
    readonly: bool = False
    index: str = 'key'
    required_columns: typing.ClassVar[typing.List[str]] = [
        'key',
        'serializer',
        'load_kwargs',
        'dump_kwargs',
    ]

    @abc.abstractmethod
    def put(self, key, value):
        ...

    @abc.abstractmethod
    def get(self, key):
        ...


@pydantic.dataclasses.dataclass
class MemoryMetadataStore(BaseMetadataStore):
    def __post_init_post_parse__(self):
        self.df = pd.DataFrame(columns=self.required_columns).set_index(self.index)

    def put(self, key: str, value):
        self.df.loc[key] = value

    def get(self, key: str):
        return self.df.loc[key]
