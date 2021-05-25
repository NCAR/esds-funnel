import typing

import intake
import pydantic

from .config import Settings

_default_settings = Settings()


@pydantic.dataclasses.dataclass
class Collection:
    esm_collection_json: typing.Union[pydantic.FilePath, pydantic.AnyUrl]
    query: typing.Dict[str, typing.Any]
    postprocess: typing.List[typing.Callable] = None
    persist: bool = False
    settings: Settings = _default_settings

    def __post_init_post_parse__(self):
        self.catalog = intake.open_esm_datastore(self.esm_collection_json)
        self._variable_column_name = self.catalog.variable_column_name
        self._query_without_vars = self.query.copy()
        self._requested_variables = self._query_without_vars.pop(self._variable_column_name)
        self._base_variables = self.catalog.unique(self._variable_column_name)[
            self._variable_column_name
        ]['values']
