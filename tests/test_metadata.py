import pandas as pd
import pytest
import xarray as xr

from funnel import CacheStore, MemoryMetadataStore, SQLMetadataStore

ds = xr.tutorial.open_dataset('tiny')


@pytest.mark.parametrize('metadata_store', [MemoryMetadataStore, SQLMetadataStore])
@pytest.mark.parametrize(
    'key, value, serializer, dump_kwargs',
    [
        ('test', {'a': [1, 2, 3], 'b': 'foo'}, 'auto', {}),
        ('tiny', ds, 'xarray.netcdf', {}),
        ('tiny_zarr', ds, 'xarray.zarr', {'mode': 'w'}),
    ],
)
def test_memory_metadata_store(metadata_store, key, value, serializer, dump_kwargs):

    ms = metadata_store(CacheStore())
    assert isinstance(ms.df, pd.DataFrame)
    ms.put(key, value, serializer, dump_kwargs=dump_kwargs)
    results = ms.get(key)
    assert type(results) == type(value)
