# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Float,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

from ...domain.model import *
from ...domain.value import *

from sqlalchemy.orm import relationship

Base = declarative_base()

class SourceTableEntrySql(Base):
    __tablename__ = "__table_info__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    content__remote_service = Column(String, nullable=False)
    content__remote_title = Column(String, nullable=False)
    content__last_fetched_utc = Column(DateTime, nullable=False)
    content__data_checksum = Column(String, nullable=False)
    content__schema_checksum = Column(String, nullable=False)

    columns = relationship(
        "SourceColumnEntrySql",
        backref="parent_table",
        order_by="SourceColumnEntrySql.id",
        collection_class=attribute_mapped_collection("content__name"),
    )

    def __repr__(self):
        return "SourceTable(name: {}, columns={})".format(self.name, self.columns)

    def __init__(self, o: SourceTableEntry):
        self.id=o.id
        self.name=o.name
        self.content__remote_service=o.content.remote_service
        self.content__remote_title=o.content.remote_title
        self.content__last_fetched_utc=o.content.last_fetched_utc
        self.content__data_checksum=o.content.data_checksum
        self.content__schema_checksum=o.content.schema_checksum

class SourceColumnEntrySql(Base):
    __tablename__ = "__column_info__"
    id = Column(Integer, primary_key=True)
    parent_table_id = Column(Integer, ForeignKey(SourceTableEntrySql.id), nullable=False)
    content__name = Column(String, nullable=False)
    content__type = Column(String, nullable=False)
    content__prompt = Column(String)
   
    def __init__(self, o: SourceColumnEntry):
        self.id=o.id
        self.parent_table_id=o.parent_table_id
        self.content__name=o.content.name
        self.content__type=o.content.type
        self.content__prompt=o.content.prompt

sql_source_column_lookup: t.Mapping[SourceColumnTypeStr, t.Type[t.Any]] = {
    'bool': Integer,
    'ordinal': Integer,
    'real': Float,
    'text': String,
    'integer': Integer,
}