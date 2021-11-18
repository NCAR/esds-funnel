import pytest
import xarray as xr

from funnel import CacheStore, SQLMetadataStore
from funnel.prefect.result import FunnelResult

ds = xr.tutorial.open_dataset('air_temperature').isel(time=0)


@pytest.mark.parametrize(
    'data, serializer',
    [
        ([1, 2, 3], 'auto'),
        ({'foo': 1, 'bar': 2}, 'auto'),
        (ds, 'xarray.netcdf'),
        ((ds, 'xarray.zarr')),
    ],
)
def test_result(data, serializer):
    r = FunnelResult(SQLMetadataStore(CacheStore(), serializer=serializer))
    new = r.write(data)
    assert new.read(new.location).value == data
    assert r.metadata_store.get(new.location) == data
