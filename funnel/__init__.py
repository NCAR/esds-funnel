#!/usr/bin/env python3
# flake8: noqa
""" Top-level module for esds-funnel. """
from pkg_resources import DistributionNotFound, get_distribution

from .cache import CacheStore
from .collection import Collection
from .config import Settings
from .registry import registry
from .serializers import pick_serializer, serializers

try:
    __version__ = get_distribution('esds-funnel').version
except DistributionNotFound:  # pragma: no cover
    __version__ = 'unknown'  # pragma: no cover
