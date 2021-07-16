import pandas as pd
import pydantic

from ..metadata import BaseMetadataStore


@pydantic.dataclasses.dataclass
class ESMMetdataStore(BaseMetadataStore):
    def __post_init_post_parse__(self):
        self.df = pd.DataFrame(
            columns=[
                'key',
                'serializer',
                'dump_kwargs',
                'load_kwargs',
                'esm_collection_json',
                'esm_collection_key',
                'collection_name',
                'variable',
            ]
        ).set_index('key')

    def put(self, key, value, origins_dict={}, serializer='auto', **dump_kwargs):
        # Writes, returns a dictionary used (what serializer? what dump kwargs)
        receipt = self.cache_store.put(key, value, serializer, **dump_kwargs)
        print(receipt)
        receipt_dict = receipt.dict()
        receipt_dict.update(origins_dict)
        self.df.loc[key] = receipt_dict

    def get(self, key, **load_kwargs):
        # Gives you a pandas series
        x = self.df.loc[key]

        # If this is an empty dictionary, pick the non-empty one
        load_kwargs = load_kwargs or x.load_kwargs

        return self.cache_store.get(key, x.serializer, **load_kwargs)
