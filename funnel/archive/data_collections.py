import yaml

import funnel as fn
import operators as ops
import variable_defs

with open('config.yml') as fid:
    config_dict = yaml.load(fid, Loader=yaml.Loader)


def _get_experiment_sel_dict(experiment):
    if experiment == 'historical':
        return dict(time=slice('1990', '2014'))
    elif 'SSP'  in experiment:
        return dict(time=slice('2086', '2100'))        
    else:
        raise ValueError(f'no sel_dict setting for {experiment}')

        
@fn.register_query_dependent_op(
    query_keys=['experiment'],
)
def _mean_time_for_experiment(ds, experiment):
    """compute the mean over time"""
    return ops.mean_time(
        ds, 
        sel_dict=_get_experiment_sel_dict(experiment),
    )


def epoch_mean(query, name='epoch_mean', center_time=True):
    """Instantiate a `funnel.Collection` object for computing epoch means."""
    postproccess = [_mean_time_for_experiment] 
    postproccess_kwargs = [{}]    
    if center_time:
        postproccess = [ops.center_time] + postproccess
        postproccess_kwargs = [{}] + postproccess_kwargs

    return fn.Collection(
        name=name,
        esm_collection_json=config_dict['esm_collection'],
        postproccess=postproccess,  
        postproccess_kwargs=postproccess_kwargs,
        query=query,
        cache_dir=config_dict['cache_dir'],
        persist=True,
        cdf_kwargs=dict(chunks={'time': 4}), 
    )


def global_mean_timeseries_ann(query, name='global_mean_timeseries_ann',
                               center_time=True):
    """
    Instantiate a `funnel.Collection` object for computing 
    global mean, annual mean timeseries.
    """
    
    postproccess = [ops.global_mean, ops.resample_ann] 
    postproccess_kwargs = [dict(normalize=True, include_ms=False), {}]    
    
    if center_time:
        postproccess = [ops.center_time] + postproccess
        postproccess_kwargs = [{}] + postproccess_kwargs

    return fn.Collection(
        name=name,
        esm_collection_json=config_dict['esm_collection'],
        postproccess=postproccess,  
        postproccess_kwargs=postproccess_kwargs,
        query=query,
        cache_dir=config_dict['cache_dir'],
        persist=True,
        cdf_kwargs=dict(chunks={'time': 4}, decode_coords=False), 
    )    

def global_integral_timeseries_ann(query, name='global_mean_timeseries_ann',
                               center_time=True):
    """
    Instantiate a `funnel.Collection` object for computing 
    global mean, annual mean timeseries.
    """
    
    postproccess = [ops.global_mean, ops.resample_ann] 
    postproccess_kwargs = [dict(normalize=False, include_ms=False), {}]    
    
    if center_time:
        postproccess = [ops.center_time] + postproccess
        postproccess_kwargs = [{}] + postproccess_kwargs

    return fn.Collection(
        name=name,
        esm_collection_json=config_dict['esm_collection'],
        postproccess=postproccess,  
        postproccess_kwargs=postproccess_kwargs,
        query=query,
        cache_dir=config_dict['cache_dir'],
        persist=True,
        cdf_kwargs=dict(chunks={'time': 4}, decode_coords=False), 
    ) 
