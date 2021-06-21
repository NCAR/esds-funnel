import os
from glob import glob
import shutil
import pprint

import traceback
import warnings
import json 
import yaml

import intake

import dask
import xarray as xr

from toolz import curry

from . config import cache_catalog_dir, cache_catalog_prefix
from . registry import _DERIVED_VAR_REGISTRY, _QUERY_DEPENDENT_OP_REGISTRY

os.makedirs(cache_catalog_dir, exist_ok=True)


class Collection(object):
    """
    Catalog + Query + Operation = Unique Dataset
    
    Extend an intake-esm catalog with the ability to:
      (1) Compute derived variables combining multipe catalog entries;
      (2) Apply postprocessing operations to returned datasets;
      (3) Optionally cache the results and curate a discrete catalog of processed assets; 
          skip computation on subsequent calls for existing cataloged assets.

    Parameters
    ----------
    
    name : str
      Name of this collection.
      
    catalog : str
      JSON file defining `intake-esm` catalog
      
    query : dict
      Dictionary of keyword, value pairs defining a query subsetting the `catalog`.
          
    postproccess : callable, list of callables, optional
      Call these functions on each dataset following aggregation.
    
    postproccess_kwargs: dict, list of dict's, optional
       Keyword arguments to `postprocess` functions.
       **Note** These arguments must be serializable to yaml.
    
    persist : bool, optional
       Persist datasets to disk.
       
    cache_dir : str, optional
       Directory for storing cache files.
       
    cache_format : str
       Format in which to store cache files; can be "nc" or "zarr"
       
    kwargs : dict
       Defaults for keyword arguments to pass to `intake_esm.core.esm_datastore.to_dataset_dict`.
       (Note: these can be updated/overridden for each call to self.dsets below.)
    """
    def __init__(self, 
                 name,
                 esm_collection_json, 
                 query, 
                 postproccess=[], 
                 postproccess_kwargs=[], 
                 persist=False,
                 cache_dir='.', 
                 cache_format='nc',    
                 **kwargs,
                ):
       
        self.name = name
    
        # TODO: what should we do if "variable" is in the query?
        #       for now, just remove it
        variable = query.get('variable', None)
        if variable is not None:            
            warnings.warn('removed "variable" key from query')
    
        # TODO: accept multiple catalogs and concatenate them into one?
        self.catalog = intake.open_esm_datastore(esm_collection_json).search(**query)
    
        # setup cache info
        assert cache_format in ['nc', 'zarr'], f'unsupported format {cache_format}'             
        self._format = cache_format        
        self.persist = persist        
        self.cache_dir = cache_dir         
        if self.persist and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        self._open_cache_kwargs = dict() # TODO: get this from config?
        if self._format == 'zarr' and 'consolidated' not in self._open_cache_kwargs:
            self._open_cache_kwargs['consolidated'] = True
        
        self._to_dsets_kwargs_defaults = kwargs
        
        # setup operators attrs
        if not isinstance(postproccess, list):
            postproccess = [postproccess]
        self.operators = postproccess
 
        if not postproccess_kwargs:
            self.ops_kwargs = [{} for op in self.operators]
        else:
            if not isinstance(postproccess_kwargs, list):
                postproccess_kwargs = [postproccess_kwargs]            
            assert len(postproccess_kwargs) == len(postproccess), 'mismatched ops/ops_kwargs'
            self.ops_kwargs = postproccess_kwargs
        
        # pull out "prepocess"
        preprocess_name = None if 'preprocess' not in kwargs else kwargs['preprocess'].__name__ 
        
        # build origins dict
        # TODO: save more about the function, including the source code 
        #       rather than simply the name
        self.origins_dict = dict(
            name=name,
            esm_collection=esm_collection_json, 
            query=query, 
            preprocess=preprocess_name,
            operators=[op.__name__ for op in self.operators],
            operator_kwargs=self.ops_kwargs,
        )                
        
        # assemble database of existing cache's
        self._assemble_cache_db()
        
    def _assemble_cache_db(self):
        """loop over yaml files in cache dir; find ones that match"""
        self._cache_files = {}        
        if not self.persist:
            return
        
        cache_id_files = self._find_cache_id_files()
        
        if cache_id_files:
            for file in cache_id_files:
                try:
                    with open(file, 'r') as fid:
                        cache_id = yaml.load(fid, Loader=yaml.Loader)
                except: # TODO: be more informative here
                    print(f'cannot read cache: {file}...skipping')
                    print('skipping')
                    
                cache_origins_dict = {
                    k: cache_id[k] 
                    for k in self.origins_dict.keys()
                    if k in cache_id
                }
                if cache_origins_dict == self.origins_dict:
                    variable = cache_id['variable']
                    key = cache_id['key']
                    if variable not in self._cache_files:
                        self._cache_files[variable] = {}                        
                    self._cache_files[variable][key] = cache_id['asset']
            
    def to_dataset_dict(self, variable, compute=True, clobber=False, 
                        prefer_derived=False, **kwargs):
        """
        Get dataset_dicts for assets.
        
        Parameters
        ----------
        
        variable : str or list
          The variable or list of variables to read and process.
          
        compute : bool, optional (default=True)
          Specify whether to call compute on the final datasets.
          
        clobber : bool, optional (default=False)
          Remove and recreate any existing cache.
          
        kwargs : dict, optional 
          Keyword arguments to pass to `intake_esm.core.esm_datastore.to_dataset_dict`
        
        """
        
        # TODO: `variable` could be more flexible, 
        # i.e. it could be a query dict with a require "variable" key
        
        if isinstance(variable, list):
            dsets_list = [
                self.to_dataset_dict(v, compute, clobber, prefer_derived, **kwargs) 
                for v in variable
            ]
            keys = list(set([k for dsets in dsets_list for k in dsets.keys()]))
            dsets_merged = {}
            for key in keys:
                ds_list = []
                for dsets in dsets_list:                    
                    if key in dsets:
                        ds_list.append(dsets[key])
                dsets_merged[key] = xr.merge(ds_list)
            return dsets_merged        
        
        # TODO: check for lock file, wait if present        
        if self._cache_exists(variable, clobber):
            return self._read_cache(variable)
        
        else:
            # TODO: set lock file
            to_dsets_kwargs = kwargs.copy()
            to_dsets_kwargs.update(self._to_dsets_kwargs_defaults)            
            # TODO: optionally spin up a cluster here
            return self._generate_dsets(
                variable, compute, prefer_derived, **to_dsets_kwargs
            )
            # TODO: release lock
        
    def _generate_dsets(self, variable, compute, prefer_derived, **kwargs):
        """Do the computation necessary to make `dsets`"""
        
        # check for variable in catalog
        # TODO: we're handling empty results below: 
        #       suppress intake-esm warning or change logic
        catalog_subset_var = self.catalog.search(variable=variable)            

        # check for variable in derived registry
        is_derived = variable in _DERIVED_VAR_REGISTRY
    
        if len(catalog_subset_var):
            if is_derived and not prefer_derived:
                warnings.warn(
                    f'found variable "{variable}" in catalog and derived_var registry'
                )
                is_derived = False                
        else:
            if not is_derived:
                raise ValueError(f'variable not found {variable}')
        
        if is_derived:
            derived_var_obj = _DERIVED_VAR_REGISTRY[variable]
            query_vars = derived_var_obj.dependent_vars
            catalog_subset_var = self.catalog.search(variable=query_vars)

        dsets = catalog_subset_var.to_dataset_dict(**kwargs)
        key_info = _intake_esm_get_keys_info(catalog_subset_var)
        
        if is_derived:
            for key, ds in dsets.items():
                dsets[key] = derived_var_obj(ds)

        for key in dsets.keys():
            for op, kw in zip(self.operators, self.ops_kwargs):
                if hash(op) in _QUERY_DEPENDENT_OP_REGISTRY:
                    op_obj = _QUERY_DEPENDENT_OP_REGISTRY[hash(op)]             
                    dsets[key] = op_obj(
                        dsets[key], 
                        query_dict=key_info[key],
                        **kw,
                    )
                else:
                    dsets[key] = op(dsets[key], **kw)

        if compute:
            dsets = {k: ds.compute() for k, ds in dsets.items()}

        if self.persist:
            self._make_cache(dsets, variable)

        return dsets  
    
    def _make_cache(self, dsets, variable):
        """write cache file"""
        
        cache_files = {variable: {}}
        for key, ds in dsets.items():
            
            cache_id_dict = self.origins_dict.copy()
            cache_id_dict['key'] = key
            cache_id_dict['variable'] = variable
            cache_id_dict['asset'] = self._gen_cache_file_name(key, variable)
            
            cache_id_file = self._gen_cache_id_file_name(key, variable)
            
            if self._format == 'nc':
                ds.to_netcdf(cache_id_dict['asset'])
            elif self._format == 'zarr':
                ds.to_zarr(cache_id_dict['asset'], mode='w', consolidated=True)
            
            with open(cache_id_file, 'w') as fid:
                yaml.dump(cache_id_dict, fid)
            
            cache_files[variable][key] = cache_id_dict['asset']
            
        self._cache_files.update(cache_files)
        
    def _read_cache(self, variable):            
        """read cache files"""
        dsets = {}
        for key, asset in self._cache_files[variable].items():
            if self._format == 'nc':
                with xr.open_dataset(asset, **self._open_cache_kwargs) as ds:
                    dsets[key] = ds
            elif self._format == 'zarr':
                with xr.open_zarr(asset, **self._open_cache_kwargs) as ds:
                    dsets[key] = ds
        return dsets

    def _cache_exists(self, variable, clobber):        
        """determine if cache files exist (or clobber them)"""
        
        if variable in self._cache_files:
            if clobber:
                for asset in self._cache_files[variable].values():
                    if os.path.exists(asset):
                        self._remove_asset(asset)
                return False
            else:
                return all(
                    [os.path.exists(asset) for asset in self._cache_files[variable].values()]
                )
        else:
            return False
    
    def _remove_asset(self, asset):
        """delete asset from disk"""
        if not os.path.exists(asset):
            return
        if self._format == 'nc':
            os.remove(asset)
        elif self._format == 'zarr':
            shutil.rmtree(asset)
        
    def _gen_cache_file_name(self, key, variable):
        """generate a file cache name"""
        # TODO: accept a user-provided callable to generate human-readable
        #       file name
        token = dask.base.tokenize(self.origins_dict, key, variable)
        return f'{self.cache_dir}/{token}.{self._format}'
    
    def _gen_cache_id_file_name(self, key, variable):
        """generate a unique cache file name"""
        token = dask.base.tokenize(self.origins_dict, key, variable)
        return f'{cache_catalog_dir}/{cache_catalog_prefix}-{token}.yml'
        
    def _find_cache_id_files(self):
        return sorted(glob(f'{cache_catalog_dir}/{cache_catalog_prefix}-*.yml'))        

    def __repr__(self):
        return pprint.pformat(self.origins_dict, indent=2, width=1)
        
class derived_var(object):
    """
    Support computation of variables that depend on multiple variables, 
    i.e., "derived vars"
    """
    def __init__(self, dependent_vars, func):
        self.dependent_vars = dependent_vars
        self._callable = func                
        
    def __call__(self, ds, **kwargs):
        """call the function to compute derived var"""
        self._ensure_variables(ds)
        return self._callable(ds, **kwargs)
            
    def _ensure_variables(self, ds):
        """ensure that required variables are present"""
        missing_var = set(self.dependent_vars) - set(ds.variables)
        if missing_var:
            raise ValueError(f'Variables missing: {missing_var}')


class query_dependent_op(object):
    """
    Support calling functions that depend on the values of the query.
    """
    def __init__(self, func, query_keys):
        self._callable = func
        self._query_keys = query_keys
    
    def __call__(self, ds, query_dict, **kwargs):
        """call function with query keys added to keyword args"""
        kwargs.update({k: query_dict[k] for k in self._query_keys})
        return self._callable(ds, **kwargs)
    
    
@curry
def register_derived_var(func, varname, dependent_vars):
    """register a function for computing derived variables"""
    if varname in _DERIVED_VAR_REGISTRY:
        warnings.warn(
            f'overwriting derived variable "{varname}" definition'
        )

    _DERIVED_VAR_REGISTRY[varname] = derived_var(
        dependent_vars, func,
    )    
    return func


@curry
def register_query_dependent_op(func, query_keys):
    """register a function for computing derived variables"""
    func_hash = hash(func)
    if func_hash in _QUERY_DEPENDENT_OP_REGISTRY:
        warnings.warn(
            f'overwriting query dependent operator "{func.__name__}" definition'
        )

    _QUERY_DEPENDENT_OP_REGISTRY[func_hash] = query_dependent_op(
        func, query_keys,
    )    
    return func


def _intake_esm_get_keys_info(cat):
    """return a dictionary with the values of components of the keys in an
       intake catalog
    """
    groupby_attrs_values = {
        k: cat.unique(columns=k)[k]['values'] 
        for k in cat.groupby_attrs
    }        
    key_info = {k: {} for k in cat.keys()}
    for key in cat.keys():
        for attr in cat.groupby_attrs:
            values = groupby_attrs_values[attr]
            match = [v for v in values if v in key]
            assert len(match) == 1, 'expecting a unique match'
            key_info[key][attr] = match[0]
    return key_info

