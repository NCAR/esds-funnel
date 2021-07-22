from typing import Callable

import catalogue
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
    'registry_name,func_name',
    [
        ('serializers', 'xarray.netcdf'),
        ('serializers', 'xarray.zarr'),
        ('serializers', 'joblib'),
        ('metadata_store', 'memory'),
        ('metadata_store', 'sql'),
    ],
)
def test_get(registry_name, func_name):
    assert isinstance(funnel.registry.get(registry_name, func_name), Callable)


def test_get_error():
    with pytest.raises(ValueError):
        funnel.registry.get('my_registry', 'my_func')

    with pytest.raises(catalogue.RegistryError):
        funnel.registry.get('serializers', 'my_func')


def test_create_error():
    with pytest.raises(ValueError):
        funnel.registry.create('serializers')


def test_create():
    funnel.registry.create('my_registry')

    @funnel.registry.my_registry.register('test')
    def my_func():
        return

    assert funnel.registry.has('my_registry', 'test')
