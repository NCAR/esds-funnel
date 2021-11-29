import pytest
import xarray as xr
import xcollection as xc

import funnel


@pytest.mark.parametrize(
    'value, expected_serializer',
    [
        (['foo', 'bar'], 'joblib'),
        ({'foo': 'bar'}, 'joblib'),
        (xr.DataArray([1, 2]), 'xarray.netcdf'),
        (xr.DataArray([1, 2]).to_dataset(name='test'), 'xarray.netcdf'),
        (
            xc.Collection(
                {
                    'foo': xr.DataArray([1, 2]).to_dataset(name='test'),
                    'bar': xr.DataArray([1, 2]).to_dataset(name='test'),
                }
            ),
            'xcollection',
        ),
    ],
)
def test_default_serializer(value, expected_serializer):
    assert funnel.pick_serializer(value) == expected_serializer
