from typing import Callable

import pytest

import funnel


@pytest.mark.parametrize(
    'registry_name,func_name,expected',
    [
        ('serializers', 'xarray.netcdf', True),
        ('serializers', 'my_func', False),
        ('my_registry', 'test', False),
    ],
)
def test_has(registry_name, func_name, expected):
    assert funnel.registry.has(registry_name, func_name) == expected


@pytest.mark.parametrize(
    'registry_name,func_name', [('serializers', 'xarray.netcdf'), ('serializers', 'joblib')]
)
def test_get(registry_name, func_name):
    assert isinstance(funnel.registry.get(registry_name, func_name), Callable)


def test_get_error():
    with pytest.raises(ValueError):
        funnel.registry.get('my_registry', 'my_func')

    with pytest.raises(ValueError):
        funnel.registry.get('serializers', 'my_func')


def test_create_error():
    with pytest.raises(ValueError):
        funnel.registry.create('serializers')
