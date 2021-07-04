import pandas as pd
import pytest
import xarray as xr

from funnel import CacheStore, MemoryMetadataStore

ds = xr.tutorial.open_dataset('tiny')


@pytest.mark.parametrize('cache_store', ['.'])
@pytest.mark.parametrize(
    'key, value, serializer, dump_kwargs',
    [
        ('test', {'a': [1, 2, 3], 'b': 'foo'}, 'auto', {}),
        ('tiny', ds, 'xarray.netcdf', {}),
        ('tiny_zarr', ds, 'xarray.zarr', {'mode': 'w'}),
    ],
)
def test_memory_metadata_store(tmp_path, cache_store, key, value, serializer, dump_kwargs):

    ms = MemoryMetadataStore(CacheStore(str(tmp_path / cache_store)))
    assert isinstance(ms.df, pd.DataFrame)
    ms.put(key, value, serializer, **dump_kwargs)
    results = ms.get(key)
    assert type(results) == type(value)
