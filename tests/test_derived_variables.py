import os
import pathlib

import intake
import pytest
import xarray as xr

from funnel.collection.derive_variables import (
    derived_variable_registry,
    query_dependent_operator_registry,
    register_derived_variable,
    register_query_dependent_operator,
)

root_directory = pathlib.Path(os.path.abspath(os.path.dirname(__file__))).parent


@pytest.mark.parametrize('varname, dependent_var', [('air_temperature_degC', 'air')])
def test_registry(varname, dependent_var):
    @register_derived_variable(
        varname=varname,
        dependent_vars=[dependent_var],
    )
    def calculate_degC(ds):
        """compute temperature in degC"""

        ds['air_temperature_degC'] = ds.air - 273.15
        ds.air_temperature_degC.attrs['units'] = 'degC'
        if 'coordinates' in ds.air.attrs:
            ds.air_temperature_degC.attrs['coordinates'] = ds.air.attrs['coordinates']
            ds.air_temperature_degC.encoding = ds.air.encoding
        return ds.drop_vars(['air'])

    assert len(derived_variable_registry) == 1


@pytest.mark.parametrize(
    'varname, ds', [('air_temperature_degC', xr.tutorial.open_dataset('air_temperature'))]
)
def test_derive_degC(varname, ds):
    """Test whether we can calculate a derived variable"""
    processed_ds = derived_variable_registry[varname](ds)

    assert processed_ds.air_temperature_degC.units == 'degC'


# Test out a query dependent operator - set a helper function
def _get_experiment_sel_dict(experiment):
    if experiment == 'RCP85':
        return dict(time='2020')
    else:
        raise ValueError(f'no sel_dict setting for {experiment}')


@pytest.mark.parametrize('query_key, experiment', [('experiment', 'RCP85')])
def test_query_dependent_operator(query_key, experiment):
    @register_query_dependent_operator(
        query_keys=[query_key],
    )
    def subset_time(ds, experiment):
        """compute the mean over time"""
        return ds.sel(_get_experiment_sel_dict(experiment))

    op_obj = query_dependent_operator_registry[hash(subset_time)]
    col = intake.open_esm_datastore(f'{root_directory}/data/cesm-le-test-catalog.json')
    dsets = col.search(variable='FLNS').to_dataset_dict(
        zarr_kwargs={'consolidated': True}, storage_options={'anon': True}
    )
    ds = dsets['atm.RCP85.monthly']

    # Make sure the function was registered to the registry
    assert len(query_dependent_operator_registry) == 1

    # Test to see if time was subset properly
    assert len(op_obj(ds, query_dict=dict(experiment=experiment)).time) == 12
