import pathlib
import typing

import dask
import pkg_resources
import pydantic
import yaml

from .metadata_db.main import MemoryMetadataStore, SQLMetadataStore
from .registry import registry

default_config_dir = pathlib.Path('~').expanduser() / '.config' / 'funnel'
default_config_dir.mkdir(parents=True, exist_ok=True)

config_file_path = pathlib.Path(pkg_resources.resource_filename('funnel', 'funnel.yaml'))
default_config_path = default_config_dir / 'funnel.yaml'

if not default_config_path.exists():
    default_config_path.write_text(config_file_path.read_text())


class Config(pydantic.BaseSettings):
    """
    Configuration settings for the Funnel.
    """

    metadata_store: typing.Union[SQLMetadataStore, MemoryMetadataStore] = None

    class Config:
        validate_assignment = True

    def load_config(self, config_path: str):
        """
        Loads the config from a file.
        """
        with open(config_path) as f:
            config = dask.config.expand_environment_variables(yaml.safe_load(f))
        metadata_store_type = config['metadata_store']['type']
        del config['metadata_store']['type']
        self.metadata_store = registry.metadata_store.get(metadata_store_type)(
            cache_store_options=config['cache_store'],
            metadata_store_options=config['metadata_store'],
        )


config = Config()
config.load_config(default_config_path)
