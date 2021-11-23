import abc
import tempfile
import typing

import pandas as pd
import pydantic
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy_utils import create_database, database_exists

from ..cache import CacheStore
from ..registry import registry
from . import models, schemas


@pydantic.dataclasses.dataclass
class BaseMetadataStore(abc.ABC):
    """Records metadata information about how assets/artifacts are
    cached in the cache store.

    Notes
    -----
    This could be expanded to record provenance of cached assets/artifacts
    """

    cache_store: CacheStore
    readonly: bool
    serializer_dump_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    serializer_load_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)

    @abc.abstractmethod
    def put(self, key, value, **dump_kwargs) -> None:
        ...

    @abc.abstractmethod
    def get(self, key, **load_kwargs) -> typing.Any:
        ...

    @abc.abstractmethod
    def __contains__(self, key: str) -> bool:
        ...

    @property
    @abc.abstractmethod
    def df(self) -> pd.DataFrame:
        ...


@pydantic.dataclasses.dataclass
class MemoryMetadataStore(BaseMetadataStore):
    """Records metadata information in an in-memory pandas DataFrame."""

    readonly: bool = False
    serializer: str = 'auto'
    serializer_dump_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    serializer_load_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)

    def __post_init_post_parse__(self):
        self.columns = [
            'key',
            'serializer',
            'load_kwargs',
            'dump_kwargs',
            'custom_fields',
            'checksum',
            'created_at',
        ]
        self._df = pd.DataFrame(columns=self.columns).set_index('key')

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    def put(
        self, key: str, value, serializer: str = 'auto', dump_kwargs: typing.Optional[dict] = None
    ) -> None:
        """Records and serializes key with its corresponding value in the metadata and cache store.

        Parameters
        ----------
        key : str

        value : typing.Any
            Any serializable Python object
        serializer : str
            The name of the serializer you want to use. The built-in
            serializers are:

                - 'auto' (default): automatically choose the serializer based on the type of the value
                - 'xarray.netcdf': requires xarray and netCDF4
                - 'xarray.zarr': requires xarray and zarr

            You can also register your own serializer via the @funnel.registry.serializers.register decorator.
        dump_kwargs : dict
            Additional keyword arguments to pass to the serializer when dumping artifact to the cache store.
        """

        serializer = serializer or self.serializer
        dump_kwargs = dump_kwargs or self.serializer_dump_kwargs
        artifact = self.cache_store.put(key, value, serializer, dump_kwargs=dump_kwargs)
        if key not in self:
            self._df.loc[key] = artifact.dict()

    def get(self, key: str, **load_kwargs) -> typing.Any:
        """Returns the value for the key if the key is in both the metadata and cache stores.

        Parameters
        ----------
        key : str
        load_kwargs : dict
            Additional keyword arguments to pass to the serializer when loading artifact from the cache store.

        Returns
        -------
        value :
            the value for the key if the key is in both the metadata and cache stores.
        """
        x = self._df.loc[key]
        _load_kwargs = load_kwargs or self.serializer_load_kwargs or x.load_kwargs
        return self.cache_store.get(key, x.serializer, **_load_kwargs)

    def __contains__(self, key: str) -> bool:
        return key in self._df.index


@pydantic.dataclasses.dataclass
class CacheMetadataStore(BaseMetadataStore):
    """Uses the cache_store as a database"""

    readonly: bool = False
    serializer: str = 'auto'
    serializer_dump_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    serializer_load_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)

    def put(
        self, key: str, value, serializer: str = 'auto', dump_kwargs: typing.Optional[dict] = None
    ) -> None:
        """Records and serializes key with its corresponding value in the metadata and cache store.

        Parameters
        ----------
        key : str

        value : typing.Any
            Any serializable Python object
        serializer : str
            The name of the serializer you want to use. The built-in
            serializers are:

                - 'auto' (default): automatically choose the serializer based on the type of the value
                - 'xarray.netcdf': requires xarray and netCDF4
                - 'xarray.zarr': requires xarray and zarr

            You can also register your own serializer via the @funnel.registry.serializers.register decorator.
        dump_kwargs : dict
            Additional keyword arguments to pass to the serializer when dumping artifact to the cache store.
        """

        serializer = serializer or self.serializer
        dump_kwargs = dump_kwargs or self.serializer_dump_kwargs
        self.cache_store.put(key, value, serializer, dump_kwargs=dump_kwargs)

    def get(self, key: str, **load_kwargs) -> typing.Any:
        """Returns the value for the key if the key is in both the metadata and cache stores.

        Parameters
        ----------
        key : str
        load_kwargs : dict
            Additional keyword arguments to pass to the serializer when loading artifact from the cache store.

        Returns
        -------
        value :
            the value for the key if the key is in both the metadata and cache stores.
        """
        # note: in an effort to avoid using a database/dataframe, I've made two primary simplifications here:
        # 1. I don't use the artifacts predetermine load_kwargs
        # 2. I've hard coded the serialzer
        _load_kwargs = load_kwargs or self.serializer_load_kwargs or {}
        return self.cache_store.get(key, 'xarray.zarr', **_load_kwargs)

    def __contains__(self, key: str) -> bool:
        return key in self.cache_store

    @property
    def df(self) -> pd.DataFrame:
        return NotImplemented


@pydantic.dataclasses.dataclass
class SQLMetadataStore(BaseMetadataStore):
    """
    A metadata store that uses SQLAlchemy to store artifact metadata
    in a SQL database (PostgreSQL, MySQL, SQLite).

    Parameters
    ----------
    database_url : str
        The database URL to use.
    readonly : bool
        Whether the metadata store is readonly.
    serializer : str
        The name of the serializer you want to use. The built-in
        serializers are:

            - 'auto' (default): automatically choose the serializer based on the type of the value
            - 'xarray.netcdf': requires xarray and netCDF4
            - 'xarray.zarr': requires xarray and zarr

        You can also register your own serializer via the @funnel.registry.serializers.register decorator.
    serializer_load_kwargs : dict
        The load kwargs to use when loading artifacts from the cache store.
    serializer_dump_kwargs : dict
        The dump kwargs to use when dumping artifacts to the cache store.
    """

    database_url: str = f'sqlite:///{tempfile.gettempdir()}/funnel.db'
    readonly: bool = False
    serializer: str = 'auto'
    serializer_dump_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    serializer_load_kwargs: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)

    def __post_init_post_parse__(self):

        if not self.readonly and not database_exists(self.database_url):
            create_database(self.database_url)
            models.Base.metadata.create_all(self._engine)

    @property
    def _engine(self):
        return create_engine(
            self.database_url, connect_args={'check_same_thread': False}, poolclass=NullPool
        )

    @property
    def _session_factory(self):
        return sessionmaker(bind=self._engine, autocommit=False, autoflush=False)

    def __contains__(self, key: str) -> bool:
        with self._session_factory() as session:
            return bool(session.query(models.Artifact).filter_by(key=key).first())

    def get(self, key: str, **load_kwargs) -> typing.Any:
        """
        Get the metadata from the database.

        Parameters
        ----------
        key : str
        load_kwargs : dict
            Additional keyword arguments to pass to the serializer when loading artifact from the cache store.

        """
        load_kwargs = load_kwargs or self.serializer_load_kwargs
        with self._session_factory() as session:
            artifact = session.query(models.Artifact).filter_by(key=key).first()
            if artifact is None:
                raise KeyError(f'{key} not found in database')
            artifact = schemas.Artifact.from_orm(artifact)
            return self.cache_store.get(key, artifact.serializer, **load_kwargs)

    def put(
        self,
        key: str,
        value: typing.Any,
        serializer: str = None,
        dump_kwargs: typing.Dict[str, typing.Any] = None,
        custom_fields: typing.Dict[str, typing.Any] = None,
    ) -> None:
        """
        Create and record a new artifact in the database.

        Parameters
        ----------
        key : str
        value : typing.Any
            Any serializable Python object
        serializer : str
            The name of the serializer you want to use. The built-in
            serializers are:

                - 'auto' (default): automatically choose the serializer based on the type of the value
                - 'xarray.netcdf': requires xarray and netCDF4
                - 'xarray.zarr': requires xarray and zarr

            You can also register your own serializer via the @funnel.registry.serializers.register decorator.
        dump_kwargs : dict
            Additional keyword arguments to pass to the serializer when dumping artifact to the cache store.
        custom_fields : dict
            A dict with types that serialize to json. These fields can be used for searching artifacts in the metadata store.

        """
        serializer = serializer or self.serializer
        dump_kwargs = dump_kwargs or self.serializer_dump_kwargs
        custom_fields = custom_fields or {}
        artifact = self.cache_store.put(
            key, value, serializer, dump_kwargs=dump_kwargs, custom_fields=custom_fields
        )
        db_artifact = models.Artifact(**artifact.dict())
        with self._session_factory() as session:
            if key not in self:
                session.add(db_artifact)
                session.commit()
                session.refresh(db_artifact)
                return db_artifact

    @property
    def df(self) -> pd.DataFrame:
        """
        Return a pandas DataFrame of the metadata stored in the database.
        """
        with self._session_factory() as session:
            return pd.read_sql_table(
                models.Artifact.__tablename__,
                con=session.connection(),
                index_col=models.Artifact.key.name,
            )


@registry.metadata_store('memory')
def memory_metadata_store(cache_store_options: dict, metadata_store_options: dict):
    cache_store = CacheStore(**cache_store_options)
    return MemoryMetadataStore(cache_store, **metadata_store_options)


@registry.metadata_store('sql')
def sql_metadata_store(cache_store_options: dict, metadata_store_options: dict):
    cache_store = CacheStore(**cache_store_options)
    return SQLMetadataStore(cache_store, **metadata_store_options)
