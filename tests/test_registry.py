from typing import Callable

import pytest

import funnel


@pytest.mark.parametrize(
    'registry_name,func_name,expected',
    [('serializers', 'xarray.netcdf', True), ('serializers', 'my_func', False)],
)
def test_has(registry_name, func_name, expected):
    assert funnel.registry.has(registry_name, func_name) == expected


@pytest.mark.parametrize(
    'registry_name,func_name', [('serializers', 'xarray.netcdf'), ('serializers', 'joblib')]
)
def test_get(registry_name, func_name):
    assert isinstance(funnel.registry.get(registry_name, func_name), Callable)
