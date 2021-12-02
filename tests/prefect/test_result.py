import os
import time

import pytest
import xarray as xr
from prefect import Flow, task
from prefect.executors import DaskExecutor, LocalExecutor

from funnel import CacheStore
from funnel.prefect.result import FunnelResult

ds = xr.tutorial.open_dataset('air_temperature').isel(time=0)


@pytest.mark.parametrize(
    'data, serializer',
    [
        ([1, 2, 3], 'auto'),
        ({'foo': 1, 'bar': 2}, 'auto'),
        (ds, 'xarray.netcdf'),
        ((ds, 'xarray.zarr')),
    ],
)
def test_result(data, serializer):
    r = FunnelResult(
        
            CacheStore(),
            serializer=serializer,
        
    )

    new = r.write(data)
    assert new.read(new.location).value == data
    assert r.cache_store.get(new.location) == data
    assert r.exists(new.location)


@pytest.mark.parametrize(
    'executor',
    [
        LocalExecutor(),
        DaskExecutor(cluster_kwargs={'processes': False, 'threads_per_worker': 8}, debug=True),
    ],
)
def test_result_flow(executor):
    os.environ['PREFECT__FLOWS__CHECKPOINTING'] = 'True'
    r = FunnelResult(
        
            CacheStore(),
            serializer='xarray.netcdf',
        
    )

    @task(target='testing.nc', result=r)
    def xarray_task():
        time.sleep(2)
        return xr.tutorial.open_dataset('rasm').isel(time=0)

    with Flow('test') as flow:
        xarray_task()

    state = flow.run(executor=executor)
    assert not state.is_failed()
