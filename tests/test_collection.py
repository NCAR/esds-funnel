import pytest
import xarray as xr

from funnel import CacheStore, SQLMetadataStore
from funnel.collection import Collection


def subset_time(ds, **kwargs):
    return ds.isel(time=range(24))


def choose_boulder(ds, **kwargs):
    return ds.sel(lat=40.015, lon=-105.2705, method='nearest')


def yearly_mean(ds, **kwargs):
    return ds.groupby('time.year').mean()


def ensemble_mean(ds, **kwargs):
    return ds.mean(dim='member_id')


@pytest.mark.parametrize(
    'metadata_store, collection_name, esm_collection_json, esm_collection_query, operators, operator_kwargs, serializer, kwargs',
    [
        SQLMetadataStore(CacheStore(path='testdb'), database_url='sqlite:///./funnel_test.db'),
        'yearly_ensemble_mean',
        '../../data/cesm-le-test-catalog.json',
        dict(component='atm'),
        [subset_time, choose_boulder, yearly_mean, ensemble_mean],
        [{}, {}, {}],
        'xarray.zarr',
        {'zarr_kwargs': {'consolidated': True}, 'storage_options': {'anon': True}},
    ],
)
@pytest.mark.parametrize('variable', ['FLNS'])
def test_get_collection_object_1var(
    metadata_store,
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
        metadata_store,
        collection_name,
        esm_collection_json,
        esm_collection_query,
        operators,
        operator_kwargs,
        serializer,
        kwargs,
    )

    assert isinstance(c.to_dataset_dict(variable=variable), xr.Dataset())


@pytest.mark.parametrize(
    'metadata_store, collection_name, esm_collection_json, esm_collection_query, operators, operator_kwargs, serializer, kwargs',
    [
        SQLMetadataStore(CacheStore(path='testdb'), database_url='sqlite:///./funnel_test.db'),
        'yearly_ensemble_mean',
        '../../data/cesm-le-test-catalog.json',
        dict(component='atm'),
        [subset_time, choose_boulder, yearly_mean, ensemble_mean],
        [{}, {}, {}],
        'xarray.zarr',
        {'zarr_kwargs': {'consolidated': True}, 'storage_options': {'anon': True}},
    ],
)
@pytest.mark.parametrize('variable', ['FLNS', 'FLNSC'])
def test_get_collection_object_2var(
    metadata_store,
    collection_name,
    esm_collection_json,
    esm_collection_query,
    operators,
    operator_kwargs,
    serializer,
    kwargs,
    variable,
):
    """Test collection for two variables"""
    c = Collection(
        metadata_store,
        collection_name,
        esm_collection_json,
        esm_collection_query,
        operators,
        operator_kwargs,
        serializer,
        kwargs,
    )

    assert isinstance(c.to_dataset_dict(variable=variable), xr.Dataset())
