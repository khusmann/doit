# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    JSON,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    backref,
    relationship,
)

Base = declarative_base()

class StudyTableSql(Base):
    __tablename__ = "__tables__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    indices = Column(JSON, nullable=False)
    columns = relationship(
        "MeasureNodeSql",
        backref="parent_table"
    )

    def __repr__(self):
        return "Table(tag: {}, indices={})".format(self.tag, self.indices)

class MeasureSql(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items = relationship(
        "MeasureNodeSql",
        backref="parent_measure",
        order_by="MeasureNodeSql.id",
    )

class CodeMapSql(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    values = Column(JSON, nullable=False)

class IndexColumnSql(Base):
    __tablename__ = "__indices__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))

class DumpableNode:
    def __repr__(self):
        return "{}(type={}, tag={})".format(
            type(self),
            self.type,
            self.tag,
        )

    def dump(self, _indent=0):
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.items.values()])
        )

class MeasureNodeSql(Base, DumpableNode):
    __tablename__ = "__measure_nodes__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_measure_id = Column(Integer, ForeignKey(MeasureSql.id))
    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))
    prompt = Column(String)
    type = Column(String, nullable=False)
    items = relationship(
        "MeasureNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="MeasureNodeSql.id",
    )

class InstrumentSql(Base):
    __tablename__ = "__instruments__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    instructions = Column(String)
    items = relationship(
        "InstrumentNodeSql",
        backref="parent_instrument",
        order_by="InstrumentNodeSql.id",
    )

class InstrumentNodeSql(Base, DumpableNode):
    __tablename__ = "__instrument_nodes__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_instrument_id = Column(Integer, ForeignKey(InstrumentSql.id))
    measure_node_id = Column(Integer, ForeignKey(MeasureNodeSql.id))
    index_column_id = Column(Integer, ForeignKey(IndexColumnSql.id))
    source_column_name = Column(String)
    type = Column(String, nullable=False)
    title = Column(String)
    prompt = Column(String)
    value = Column(String)
    items = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.id",
    )

