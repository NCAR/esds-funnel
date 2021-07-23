import os
import pathlib

import pytest

from funnel import Collection

root_directory = pathlib.Path(os.path.abspath(os.path.dirname(__file__))).parent


def subset_time(ds, **kwargs):
    return ds.isel(time=range(24))


def choose_boulder(ds, **kwargs):
    return ds.sel(lat=40.015, lon=-105.2705, method='nearest')


def yearly_mean(ds, **kwargs):
    return ds.groupby('time.year').mean()


def ensemble_mean(ds, **kwargs):
    return ds.mean(dim='member_id')


@pytest.mark.parametrize(
    'collection_name, esm_collection_json, esm_collection_query, operators, operator_kwargs, serializer, kwargs',
    [
        (
            'yearly_ensemble_mean',
            f'{root_directory}/data/cesm-le-test-catalog.json',
            dict(component='atm'),
            [subset_time, choose_boulder, yearly_mean, ensemble_mean],
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
