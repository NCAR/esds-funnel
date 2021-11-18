import typing

import pendulum
import pydantic
from prefect.engine.result import Result
from slugify import slugify

from ..metadata_db.main import MemoryMetadataStore, SQLMetadataStore


@pydantic.dataclasses.dataclass
class FunnelResult(Result):
    """
    A result class that is used to store the results of a task in a funnel Metadata store.
    """

    metadata_store: typing.Union['SQLMetadataStore', 'MemoryMetadataStore']
    kwargs: typing.Dict[str, typing.Any] = None

    def __post_init_post_parse__(self):
        self.kwargs = self.kwargs or {}
        super().__init__(**self.kwargs)

    @property
    def default_location(self) -> str:
        return 'prefect-result-' + slugify(pendulum.now('utc').isoformat())

    def read(self, location: str) -> Result:
        """
        Reads the result from the metadata store.
        """
        new = self.copy()
        new.location = location

        self.logger.debug('Starting to read result from {}...'.format(location))
        new.value = self.metadata_store.get(key=location)
        self.logger.debug('Finished reading result from {}...'.format(location))
        return new

    def write(self, value_: typing.Any, **kwargs: typing.Any) -> Result:
        """
        Writes the result to the metadata store.
        """

        new = self.format(**{})
        new.value = value_
        assert new.location is not None

        self.logger.debug('Starting to upload result to {}...'.format(new.location))
        self.metadata_store.put(key=new.location, value=new.value)
        self.logger.debug('Finished uploading result to {}.'.format(new.location))
        return new

    def exists(self, location: str, **kwargs: typing.Any) -> bool:
        """Checks whether the target result exists in the metadata store.

        Does not validate whether the result is `valid`, only that it is present.

        """
        return location in self.metadata_store


# Fixes https://github.com/samuelcolvin/pydantic/issues/704
FunnelResult.__pydantic_model__.update_forward_refs()
