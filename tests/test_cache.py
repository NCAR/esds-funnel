import fsspec
import pytest

from funnel import CacheStore


@pytest.mark.parametrize('path', ['memory://', 'file://', '~/test'])
@pytest.mark.parametrize('readonly', [True, False])
def test_initialization(path, readonly):
    store = CacheStore(path, readonly=readonly)
    assert isinstance(store.mapper, fsspec.mapping.FSMap)
    assert isinstance(store.raw_path, str)
    assert store.readonly == readonly


@pytest.mark.parametrize('key, data', [('bar/test', 'my_data'), ('foo', [1, 3, 4])])
def test_put_and_get(key, data):
    store = CacheStore('memory://')
    store.put(key, data)
    assert key in store.keys()
    results = store.get(key)
    assert results == data


@pytest.mark.parametrize('key, data', [('bar/test', 'my_data'), ('foo', [1, 3, 4])])
def test_delete(key, data):
    store = CacheStore('memory://')
    store.put(key, data)
    store.delete(key)
    assert key not in store.keys()
