import fsspec
import pytest

from funnel import CacheStore


@pytest.mark.parametrize('path', ['memory://', 'file://', '~/test'])
def test_cache_initialization(path):
    store = CacheStore(path)
    assert isinstance(store.mapper, fsspec.mapping.FSMap)


@pytest.mark.parametrize('key, data', [('bar/test', 'my_data'), ('foo', [1, 3, 4])])
def test_cache_put_and_get(key, data):
    store = CacheStore('memory://')
    store.put(key, data)
    assert key in store.keys()
    results = store.get(key)
    assert results == data
