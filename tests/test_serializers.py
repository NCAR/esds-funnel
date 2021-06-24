import pytest
import xarray as xr

import funnel


@pytest.mark.parametrize(
    'value, expected_serializer',
    [
        (['foo', 'bar'], 'joblib'),
        ({'foo': 'bar'}, 'joblib'),
        (xr.DataArray([1, 2]), 'xarray.netcdf'),
    ],
)
def test_default_serializer(value, expected_serializer):
    assert funnel.pick_serializer(value) == expected_serializer
