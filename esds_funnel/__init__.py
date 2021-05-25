#!/usr/bin/env python3
# flake8: noqa
""" Top-level module for esds-funnel. """
from pkg_resources import DistributionNotFound, get_distribution

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:  # pragma: no cover
    __version__ = 'unknown'  # pragma: no cover
