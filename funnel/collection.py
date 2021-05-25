import typing

import intake
import pydantic

from .config import Settings, settings as _default_settings


@pydantic.dataclasses.dataclass
class Collection:
    esm_collection_json: typing.Union[pydantic.FilePath, pydantic.AnyUrl]
    query: typing.Dict[str, typing.Any]
    postprocess: typing.List[typing.Callable] = None
    persist: bool = False
    settings: Settings = _default_settings

    def __post_init__post_parse__(self):
        self._whole_catalog = intake.open_esm_datastore(self.esm_collection_json)
        self.catalog = self._whole_catalog.search(**self.query)
