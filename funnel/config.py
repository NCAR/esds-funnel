import enum
import pathlib

import pydantic


class CacheFormatEnum(str, enum.Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'


default_conf_dir = pathlib.Path('~').expanduser() / '.config' / 'funnel'
default_conf_dir.mkdir(parents=True, exist_ok=True)


class Settings(pydantic.BaseSettings):
    cache_dir: pydantic.DirectoryPath = default_conf_dir
    cache_format: CacheFormatEnum = 'netcdf'
