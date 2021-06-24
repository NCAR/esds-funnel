import functools
import typing

import pydantic
import xarray as xr

from .registry import registry

registry.create('serializers', entry_points=True)


@pydantic.dataclasses.dataclass
class Serializer:
    name: str
    load: typing.Callable
    dump: typing.Callable


@registry.serializers.register('xarray.zarr')
def xarray_zarr() -> Serializer:
    return Serializer('xarray.zarr', xr.open_zarr, xr.backends.api.to_zarr)


@registry.serializers.register('xarray.netcdf')
def xarray_netcdf() -> Serializer:
    return Serializer('xarray.netcdf', xr.open_dataset, xr.backends.api.to_netcdf)


@registry.serializers.register('joblib')
def joblib() -> Serializer:
    import joblib

    return Serializer('joblib', joblib.load, joblib.dump)


@functools.singledispatch
def pick_serializer(obj) -> str:
    """Returns the id of the appropriate serializer

    Parameters
    ----------
    obj: any Python object

    Returns
    -------
    id : str
       Id of the serializer
    """

    return registry.serializers.get('joblib')().name


@pick_serializer.register(xr.Dataset)
def _(obj):
    return registry.serializers.get('xarray.netcdf')().name


@pick_serializer.register(xr.DataArray)
def _(obj):
    return registry.serializers.get('xarray.netcdf')().name
