# Lets you tokenize a lazy object
import typing
import warnings

import intake
import pydantic
from dask.base import tokenize

from ..config import config as default_config
from ..metadata_db.main import MemoryMetadataStore, SQLMetadataStore


class OriginDict(pydantic.BaseModel):
    collection_name: str
    esm_collection_json: str
    esm_collection_query: dict
    operators: list
    operator_kwargs: list
    esm_collection_key: str = None
    variable: str = None


@pydantic.dataclasses.dataclass
class Collection:
    # rename metadatastore - cache_database
    collection_name: str
    esm_collection_json: str
    esm_collection_query: dict
    operators: list = None
    operator_kwargs: list = None
    serializer: str = 'xarray.zarr'
    metadata_store: typing.Union[MemoryMetadataStore, SQLMetadataStore] = None
    kwargs: dict = None

    def __post_init_post_parse__(self):
        self.operators = self.operators or []
        self.operator_kwargs = self.operator_kwargs or [{}]
        self.metadata_store = default_config.metadata_store or self.metadata_store
        self.origins_dict = OriginDict(
            collection_name=self.collection_name,
            esm_collection_json=self.esm_collection_json,
            esm_collection_query=self.esm_collection_query,
            operators=self.operators,
            operator_kwargs=self.operator_kwargs,
        ).dict()
        self.catalog = intake.open_esm_datastore(self.esm_collection_json)
        self.variable_column_name = self.catalog.aggregation_info.variable_column_name
        self.base_variables = set(self.catalog.df.variable)
        self.variable = self.esm_collection_query.get(self.variable_column_name, None)

    def to_dataset_dict(self, variable, compute=False):
        """Returns a dictionary of datasets similar to intake-esm"""
        dsets = {}
        if isinstance(variable, str):
            variable = [variable]

        if isinstance(variable, list):
            for v in variable:
                individual_query = self.esm_collection_query.copy()
                individual_query['variable'] = v
                # individual_query = self.esm_collection_query.copy()['variable'] = v
                self.origins_dict.update(esm_collection_query=individual_query)
                subset_catalog = self.catalog.search(**individual_query)
                for catalog_key in subset_catalog.keys():
                    self.origins_dict.update(esm_collection_key=catalog_key)
                    individual_key = tokenize(self.origins_dict)
                    if individual_key in self.metadata_store:
                        dsets[catalog_key] = self.metadata_store.get(individual_key)
                    else:
                        # read in the dataset - add to the dataset dict
                        df = subset_catalog[catalog_key]

                        # Open the dataset using the prescribed key
                        ds = self.apply_operators(df(**self.kwargs).to_dask())

                        # Add this dataset to the dictionary of datasets
                        dsets[catalog_key] = ds

                        # Add this to the database
                        if ds.nbytes > 1000000:
                            warnings.warn(
                                f'Warning: Dataset you are saving is larger than 1 GB \n Total size:{ds.nbytes * 1e-6} GB'
                            )
                        self.metadata_store.put(
                            individual_key,
                            dsets[catalog_key],
                            self.serializer,
                            custom_fields=self.origins_dict,
                        )

        return dsets

    def apply_operators(self, ds):
        """Applys operators to a list of datasets"""
        for op, kw in zip(self.operators, self.operator_kwargs):
            ds = op(ds, **kw)
        return ds
