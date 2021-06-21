import traceback
import warnings

import numpy as np
import xarray as xr

import variable_defs

nmols_to_PgCyr = 1e-9 * 86400. * 365. * 12e-15


def _get_chunks_dict(obj):
    """why doesn't Xarray have this method?"""
    return {d: len(chunks) for d, chunks in zip(obj.dims, obj.chunks)}    

def _get_tb_name_and_tb_dim(ds):
    """return the name of the time 'bounds' variable and its second dimension"""
    assert 'bounds' in ds.time.attrs, 'missing "bounds" attr on time'
    tb_name = ds.time.attrs['bounds']        
    assert tb_name in ds, f'missing "{tb_name}"'    
    tb_dim = ds[tb_name].dims[-1]
    return tb_name, tb_dim


def _gen_time_weights(ds):
    """compute temporal weights using time_bound attr"""    
    tb_name, tb_dim = _get_tb_name_and_tb_dim(ds)
    chunks_dict = _get_chunks_dict(ds[tb_name])
    del chunks_dict[tb_dim]
    return ds[tb_name].compute().diff(tb_dim).squeeze().astype(float).chunk(chunks_dict)
    
    
def center_time(ds):
    """make time the center of the time bounds"""
    ds = ds.copy()
    attrs = ds.time.attrs
    encoding = ds.time.encoding
    tb_name, tb_dim = _get_tb_name_and_tb_dim(ds)
    
    ds['time'] = ds[tb_name].compute().mean(tb_dim).squeeze()
    attrs['note'] = f'time recomputed as {tb_name}.mean({tb_dim})'
    ds.time.attrs = attrs
    ds.time.encoding = encoding
    return ds
    
    
def resample_ann(ds):
    """compute the annual mean of an xarray.Dataset"""
    
    ds = center_time(ds)
    weights = _gen_time_weights(ds)
    weights = weights.groupby('time.year') / weights.groupby('time.year').sum()
   
    # ensure they all add to one
    # TODO: build support for situations when they don't, 
    # i.e. define min coverage threshold
    nyr = len(weights.groupby('time.year'))
    np.testing.assert_allclose(weights.groupby('time.year').sum().values, np.ones(nyr))
        
    # ascertain which variables have time and which don't
    tb_name, tb_dim = _get_tb_name_and_tb_dim(ds)
    time_vars = [v for v in ds.data_vars if 'time' in ds[v].dims and v != tb_name]
    other_vars = list(set(ds.variables) - set(time_vars) - {tb_name, 'time'} )

    # compute
    with xr.set_options(keep_attrs=True):        
        return xr.merge((
            ds[other_vars],         
            (ds[time_vars] * weights).groupby('time.year').sum(dim='time'),
        )).rename({'year': 'time'})    


def global_mean(ds, normalize=True, include_ms=False):
    """
    Compute the global mean on a POP dataset. 
    Return computed quantity in conventional units.
    """

    compute_vars = [
        v for v in ds 
        if 'time' in ds[v].dims and ('nlat', 'nlon') == ds[v].dims[-2:]
    ]
    other_vars = list(set(ds.variables) - set(compute_vars))

    if include_ms:
        surface_mask = ds.TAREA.where(ds.KMT > 0).fillna(0.)
    else:
        surface_mask = ds.TAREA.where(ds.REGION_MASK > 0).fillna(0.)        
    
    masked_area = {
        v: surface_mask.where(ds[v].notnull()).fillna(0.) 
        for v in compute_vars
    }

    with xr.set_options(keep_attrs=True):
        
        dso = xr.Dataset({
            v: (ds[v] * masked_area[v]).sum(['nlat', 'nlon'])
            for v in compute_vars
        })
        if normalize:
            dso = xr.Dataset({
                v: dso[v] / masked_area[v].sum(['nlat', 'nlon'])
                for v in compute_vars
            })            
        else:
            for v in compute_vars:
                if v in variable_defs.C_flux_vars:
                    dso[v] = dso[v] * nmols_to_PgCyr
                    dso[v].attrs['units'] = 'Pg C yr$^{-1}$'
                
        return xr.merge([dso, ds[other_vars]]).drop(
            [c for c in ds.coords if ds[c].dims == ('nlat', 'nlon')]
        )
    

def mean_time(ds, sel_dict):
    """compute the mean over a time range"""
    ds = ds.sel(sel_dict)
    try:
        weights = _gen_time_weights(ds)
    except AssertionError as error:
        traceback.print_tb(error.__traceback__) 
        warnings.warn('could not generate time_weights\nusing straight average')        
        return ds.sel(sel_dict).mean('time')       
    
    tb_name, _ = _get_tb_name_and_tb_dim(ds)
    time_vars = [v for v in ds.data_vars if 'time' in ds[v].dims and v != tb_name]
    other_vars = list(set(ds.variables) - set(time_vars) - {tb_name, 'time'})
    
    with xr.set_options(keep_attrs=True):
        dso = (ds[time_vars] * weights).sum('time') / weights.sum('time')
        return xr.merge([dso, ds[other_vars]])