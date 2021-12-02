import os
import typing

os.environ['PREFECT__FLOWS__CHECKPOINTING'] = 'True'

import pendulum
import pydantic
from prefect.engine.result import Result
from slugify import slugify

from ..cache import CacheStore


@pydantic.dataclasses.dataclass
class FunnelResult(Result):
    """
    A result class that is used to store the results of a task in a funnel Metadata store.
    """

    cache_store: CacheStore
    serializer: str = 'auto'
    serializer_dump_kwargs: typing.Dict[str, typing.Any] = None
    serializer_load_kwargs: typing.Dict[str, typing.Any] = None
    kwargs: typing.Dict[str, typing.Any] = None

    def __post_init_post_parse__(self):
        self.kwargs = self.kwargs or {}
        self._serializer = self.serializer
        super().__init__(**self.kwargs)
        self.serializer = self._serializer
        self.serializer_dump_kwargs = self.serializer_dump_kwargs or {}
        self.serializer_load_kwargs = self.serializer_load_kwargs or {}

    @property
    def default_location(self) -> str:
        return f"prefect-result-{slugify(pendulum.now('utc').isoformat())}"

    def read(self, location: str) -> Result:
        """
        Reads the result from the metadata store.

        Parameters
        ----------
        location : str
            The location of the result to read.

        Returns
        -------
        Result
            The result at the given location.
        """
        new = self.copy()
        new.location = location

        self.logger.debug('Starting to read result from {}...'.format(location))
        new.value = self.cache_store.get(key=location, load_kwargs=new.serializer_load_kwargs)
        self.logger.debug('Finished reading result from {}...'.format(location))
        return new

    def write(self, value_: typing.Any, **kwargs: typing.Any) -> Result:
        """
        Writes the result to the metadata store.

        Parameters
        ----------
        value_ : typing.Any
            The value to write to the metadata store.
        kwargs : typing.Any
            Additional keyword arguments
        """

        new = self.format(**{})
        new.value = value_
        assert new.location is not None

        self.logger.debug('Starting to upload result to {}...'.format(new.location))
        self.cache_store.put(key=new.location, value=new.value, serializer=self.serializer, dump_kwargs=new.serializer_dump_kwargs)
        self.logger.debug('Finished uploading result to {}.'.format(new.location))
        return new

    def exists(self, location: str, **kwargs: typing.Any) -> bool:
        """Checks whether the target result exists in the metadata store.
        Does not validate whether the result is `valid`, only that it is present.

        Parameters
        ----------
        location : str
            The location of the result to check.
        kwargs : typing.Any
            Additional keyword arguments.
        """
        return location in self.cache_store


# Fixes https://github.com/samuelcolvin/pydantic/issues/704
FunnelResult.__pydantic_model__.update_forward_refs()
