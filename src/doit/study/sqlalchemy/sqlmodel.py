from __future__ import annotations
import typing as t
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    JSON,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import declarative_base, backref

Base  = declarative_base()

class CodeMapSql(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    values = Column(JSON, nullable=False)

    column_entries: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        backref="codemap",
        order_by="ColumnEntrySql.id"
    )

class MeasureEntrySql(Base):
    __tablename__ = "__measure_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        backref="parent_measure",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries__"
    id = Column(Integer, primary_key=True)
    parent_measure_id = Column(Integer, ForeignKey(MeasureEntrySql.id))
    parent_column_id = Column(Integer, ForeignKey(id))
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))
    name = Column(String, nullable=False)

    type = Column(String, nullable=False)

    prompt = Column(String)
    title = Column(String)
    description = Column(String)

    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnEntrySql.id",
    )