import pandas as pd
import pytest

from funnel import MemoryMetadataStore


@pytest.mark.parametrize(
    'key, value', [('test', {'serializer': 'xarray.netcdf', 'load_kwargs': {}, 'dump_kwargs': {}})]
)
def test_memory_metadata_store(key, value):
    ms = MemoryMetadataStore()
    assert isinstance(ms.df, pd.DataFrame)

    ms.put(key, value)
    results = ms.get(key)
    assert isinstance(results, pd.Series)

    assert results.to_dict() == value
