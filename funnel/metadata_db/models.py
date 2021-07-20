from sqlalchemy import JSON, TIMESTAMP, Column, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

import datetime


class Artifact(Base):
    __tablename__ = 'artifacts'

    key = Column(String(256), primary_key=True, index=True)
    serializer = Column(String(256))
    load_kwargs = Column(JSON, default={})
    dump_kwargs = Column(JSON, default={})
    custom_fields = Column(JSON, default={})
    checksum = Column(String(256), nullable=True)
    created_at = Column(TIMESTAMP, default=datetime.datetime.utcnow)
