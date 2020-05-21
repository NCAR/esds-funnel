#!/usr/bin/env python3
""" Top-level module for salisfy. """
from pkg_resources import DistributionNotFound, get_distribution

try:
    __version__ = get_distribution(__name__).version  # noqa: F401
except DistributionNotFound:  # noqa: F401
    __version__ = '0.0.0'  # noqa: F401
