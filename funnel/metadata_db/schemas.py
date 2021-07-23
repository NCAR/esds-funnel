import datetime
import typing

import pydantic


class ArtifactBase(pydantic.BaseModel):
    """
    Base class for all artifacts.
    """

    key: str
    serializer: str
    load_kwargs: typing.Optional[dict] = {}
    dump_kwargs: typing.Optional[dict] = {}
    custom_fields: typing.Optional[dict] = {}
    checksum: typing.Optional[str] = None
    created_at: datetime.datetime = datetime.datetime.utcnow()


class Artifact(ArtifactBase):
    class Config:
        orm_mode = True
        validate_assignment = True
