from derive_variables import register_derived_var


@register_derived_var(
    varname='air_temperature_degC',
    dependent_vars=['air'],
)
def calculate_degC(ds):
    """compute temperature in degC"""

    ds['air_temperature_degC'] = ds.air - 273.15
    ds.air_temperature_degC.attrs['units'] = 'degC'
    if 'coordinates' in ds.air.attrs:
        ds.air_temperature_degC.attrs['coordinates'] = ds.air.attrs['coordinates']
    ds.air_temperature_degC.encoding = ds.air.encoding
    return ds.drop(['air'])
