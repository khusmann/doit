# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    JSON,
    ForeignKey,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

from ...domain.model import *
from ...domain.value import *

from sqlalchemy.orm import relationship

Base = declarative_base()

class SourceTableInfoSql(Base):
    __tablename__ = "__table_info__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    columns = relationship(
        "SourceColumnInfoSql",
        backref="parent_table",
        order_by="SourceColumnInfoSql.id",
        collection_class=attribute_mapped_collection("name"),
    )

    def __repr__(self):
        return "SourceTable(name: {}, columns={})".format(self.name, self.columns)

    def __init__(self, o: SourceTableInfo):
        self.id=o.id
        self.name=o.name

class SourceColumnInfoSql(Base):
    __tablename__ = "__column_info__"
    id = Column(Integer, primary_key=True)
    parent_table_id = Column(Integer, ForeignKey(SourceTableInfoSql.id), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    prompt = Column(String)
   
    def __init__(self, o: SourceColumnInfo):
        self.id=o.id
        self.parent_table_id=o.parent_table_id
        self.name=o.name
        self.type=o.type
        self.prompt=o.prompt

sql_source_column_lookup: t.Mapping[SourceColumnTypeStr, t.Type[t.Any]] = {
    'bool': Integer,
    'ordinal': Integer,
    'real': Float,
    'text': String,
    'integer': Integer,
}