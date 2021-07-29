from funnel.collection.derive_variables import register_derived_variable, derived_variable_registry
import pytest

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
        return ds.drop(['air'])
    
    assert len(derived_variable_registry) == 1
