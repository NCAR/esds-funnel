import functools
import typing

import catalogue
import pydantic
import xarray as xr

serializers = catalogue.create('funnel', 'serializers')


@pydantic.dataclasses.dataclass
class Serializer:
    name: str
    load: typing.Callable
    dump: typing.Callable


@serializers.register('xarray.zarr')
def xarray_zarr():
    return Serializer('xarray.zarr', xr.open_zarr, xr.backends.api.to_zarr)


@serializers.register('xarray.netcdf')
def xarray_netcdf():
    return Serializer('xarray.netcdf', xr.open_dataset, xr.backends.api.to_netcdf)


@serializers.register('joblib')
def joblib():
    import joblib

    return Serializer('joblib', joblib.load, joblib.dump)


@functools.singledispatch
def pick_serializer(obj):
    """Returns the id of the appropriate serializer

    Parameters
    ----------
    obj: any Python object

    Returns
    -------
    id : str
       Id of the serializer
    """

    return serializers.get('joblib')().name


@pick_serializer.register(xr.Dataset)
def _(obj):
    return serializers.get('xarray.netcdf')().name


@pick_serializer.register(xr.DataArray)
def _(obj):
    return serializers.get('xarray.netcdf')().name