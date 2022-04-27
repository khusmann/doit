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

class CodemapSql(Base):
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
    codemap_id = Column(Integer, ForeignKey(CodemapSql.id))
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

    instrument_entries: RelationshipProperty[t.List[InstrumentEntrySql]] = relationship(
        "InstrumentNodeSql",
        backref=backref("column_entry", remote_side=id),
        order_by="InstrumentNodeSql.id"        
    )

class InstrumentEntrySql(Base):
    __tablename__ = "__instrument_entries__"
    id = Column(Integer, primary_key=True)
#    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    instructions = Column(String)
    source_service = Column(String)
    source_title = Column(String)
    data_checksum = Column(String)
    schema_checksum = Column(String)

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        backref="parent_instrument",
        order_by="InstrumentNodeSql.id",
    )

class InstrumentNodeSql(Base):
    __tablename__ = "__instrument_nodes__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_instrument_id = Column(Integer, ForeignKey(InstrumentEntrySql.id))
    column_entry_id = Column(Integer, ForeignKey(ColumnEntrySql.id))
    source_column_name = Column(String)
    source_column_type = Column(String)
    source_prompt = Column(String)
    type = Column(String, nullable=False)

    source_value_map = Column(JSON)
    title = Column(String)
    prompt = Column(String)
    constant_value = Column(String)

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.id",
    )

