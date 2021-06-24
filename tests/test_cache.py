import fsspec
import pytest
import xarray as xr

from funnel import CacheStore


@pytest.mark.parametrize(
    'path', ['memory://', 'file://', '~/test', 'github://pydata:xarray-data@master/']
)
@pytest.mark.parametrize('readonly', [True, False])
def test_initialization(path, readonly):
    store = CacheStore(path, readonly=readonly)
    assert isinstance(store.mapper, fsspec.mapping.FSMap)
    assert isinstance(store.raw_path, str)
    assert store.readonly == readonly


@pytest.mark.parametrize('key, data', [('bar/test', 'my_data'), ('foo', [1, 3, 4])])
def test_put_and_get(key, data):
    store = CacheStore('memory://')
    store.put(key, data)
    assert key in store.keys()
    results = store.get(key, None)
    assert results == data


@pytest.mark.parametrize(
    'key, data, serializer',
    [
        ('foo', {'a': [1, 2, 3], 'b': True}, 'joblib'),
        ('test.nc', xr.DataArray([1, 2]).to_dataset(name='sst'), 'xarray.netcdf'),
        ('my_dataset.zarr', xr.DataArray([1, 2]).to_dataset(name='sst'), 'xarray.zarr'),
    ],
)
def test_put_and_get_local(key, data, serializer, tmpdir):
    store = CacheStore(str(tmpdir))
    store.put(key, data, serializer=serializer)
    results = store.get(key, serializer=serializer)
    assert results == data


@pytest.mark.parametrize('key, data', [('bar/test', 'my_data'), ('foo', [1, 3, 4])])
def test_delete(key, data):
    store = CacheStore('memory://')
    store.put(key, data)
    store.delete(key)
    assert key not in store.keys()
