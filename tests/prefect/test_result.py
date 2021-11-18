import pytest
import xarray as xr

from funnel import CacheStore, SQLMetadataStore
from funnel.prefect.result import FunnelResult

ds = xr.tutorial.open_dataset('air_temperature').isel(time=0)
r = FunnelResult(SQLMetadataStore(CacheStore()))


@pytest.mark.parametrize('data', [[1, 2, 3], {'foo': 1, 'bar': 2}])
def test_result(data):
    new = r.write(data)
    assert new.read(new.location).value == data
