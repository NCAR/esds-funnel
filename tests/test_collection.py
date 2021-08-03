import os
import pathlib

import pytest

from funnel import Collection
from funnel.collection.derive_variables import (
    derived_variable_registry,
    register_derived_variable)

root_directory = pathlib.Path(os.path.abspath(os.path.dirname(__file__))).parent


def subset_time(ds, **kwargs):
    return ds.isel(time=range(1))


def choose_boulder(ds, **kwargs):
    return ds.sel(lat=40.015, lon=-105.2705, method='nearest')


def ensemble_mean(ds, **kwargs):
    return ds.mean(dim='member_id')

@register_derived_variable(
        varname='air_temperature_degC',
        dependent_vars=['T'],
    )
def calculate_degC(ds):
    """compute temperature in degC"""
    ds['air_temperature_degC'] = ds.T - 273.15
    ds.air_temperature_degC.attrs['units'] = 'degC'
    if 'coordinates' in ds.T.attrs:
        ds.air_temperature_degC.attrs['coordinates'] = ds.T.attrs['coordinates']
        ds.air_temperature_degC.encoding = ds.T.encoding
    return ds.drop_vars(['T'])



@pytest.mark.parametrize(
    'collection_name, esm_collection_json, esm_collection_query, operators, operator_kwargs, serializer, kwargs',
    [
        (
            'yearly_ensemble_mean',
            f'{root_directory}/data/cesm-le-test-catalog.json',
            dict(component='atm'),
            [subset_time, choose_boulder, ensemble_mean],
            [{}, {}, {}, {}],
            'xarray.zarr',
            {'zarr_kwargs': {'consolidated': True}, 'storage_options': {'anon': True}},
        )
    ],
)
@pytest.mark.parametrize('variable', ['FLNS', 'FLNSC'])
def test_get_collection_object_1var(
    collection_name,
    esm_collection_json,
    esm_collection_query,
    operators,
    operator_kwargs,
    serializer,
    kwargs,
    variable,
):
    """Test collection for one variable"""
    c = Collection(
        collection_name,
        esm_collection_json,
        esm_collection_query,
        operators,
        operator_kwargs,
        serializer,
        kwargs=kwargs,
    )

    assert isinstance(c.to_dataset_dict(variable=variable), dict)

@pytest.mark.parametrize(
    'collection_name, esm_collection_json, esm_collection_query, operators, operator_kwargs, serializer, kwargs',
    [
        (
            'yearly_ensemble_mean',
            f'{root_directory}/data/cesm-le-test-catalog.json',
            dict(component='atm'),
            [subset_time, choose_boulder, ensemble_mean],
            [{}, {}, {}, {}],
            'xarray.zarr',
            {'zarr_kwargs': {'consolidated': True}, 'storage_options': {'anon': True}},
        )
    ],
)
@pytest.mark.parametrize('variable', ['air_temperature_degC'])
def test_get_collection_object_derived(
    collection_name,
    esm_collection_json,
    esm_collection_query,
    operators,
    operator_kwargs,
    serializer,
    kwargs,
    variable,
):
    """Test collection for one variable"""
    c = Collection(
        collection_name,
        esm_collection_json,
        esm_collection_query,
        operators,
        operator_kwargs,
        serializer,
        kwargs=kwargs,
    )
    dsets = c.to_dataset_dict(variable=variable)
    keys = dsets.keys()
    print(keys)
    
    assert variable in dsets['atm.20C.monthly'].variables