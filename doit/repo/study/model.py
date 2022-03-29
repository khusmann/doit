# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    JSON,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    backref,
    relationship,
)

Base = declarative_base()

class Table(Base):
    __tablename__ = "__tables__"
    id = Column(Integer, primary_key=True)
    tag = Column(String, nullable=False, unique=True)
    indices = Column(JSON, nullable=False)

    columns = relationship(
        "MeasureNode",
        backref="parent_table"
    )

    def __repr__(self):
        return "Table(tag: {}, indices={})".format(self.tag, self.indices)

class Measure(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    tag = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items = relationship(
        "MeasureNode",
        backref="parent_measure",
        order_by="MeasureNode.order",
    )
    codes = relationship(
        "CodeMap",
        backref="parent_measure",
    )

class CodeMap(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    parent_measure_id = Column(Integer, ForeignKey(Measure.id))
    tag = Column(String, nullable=False)
    values = Column(JSON, nullable=False)
    measure_nodes = relationship(
        "MeasureNode",
        backref="codes",
    )
    index = relationship(
        "Index",
        backref="codes",
        uselist=False,
    )

class Index(Base):
    __tablename__ = "__indices__"
    id = Column(Integer, primary_key=True)
    codemap_id = Column(Integer, ForeignKey(CodeMap.id))
    tag = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)

    instrument_items = relationship(
        "InstrumentNode",
        backref="index_item",
    )

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

class MeasureNode(Base, DumpableNode):
    __tablename__ = "__measure_items__"
    id = Column(Integer, primary_key=True)
    order = Column(Integer, nullable=False)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_measure_id = Column(Integer, ForeignKey(Measure.id))
    linked_table_id = Column(Integer, ForeignKey(Table.id))
    codemap_id = Column(Integer, ForeignKey(CodeMap.id))
    prompt = Column(String)
    tag = Column(String, nullable=False, unique=True)
    type = Column(String, nullable=False)
    items = relationship(
        "MeasureNode",
        backref=backref("parent_node", remote_side=id),
        order_by="MeasureNode.order",
    )

    instrument_items = relationship(
        "InstrumentNode",
        backref="measure_item",
    )

class Instrument(Base):
    __tablename__ = "__instruments__"
    id = Column(Integer, primary_key=True)
    tag = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    instructions = Column(String)

    items = relationship(
        "InstrumentNode",
        backref="parent_instrument",
        order_by="InstrumentNode.order",
    )

class InstrumentNode(Base, DumpableNode):
    __tablename__ = "__instrument_items__"
    id = Column(Integer, primary_key=True)
    order = Column(Integer, nullable=False)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_instrument_id = Column(Integer, ForeignKey(Instrument.id))
    measure_item_id = Column(Integer, ForeignKey(MeasureNode.id))
    index_item_id = Column(Integer, ForeignKey(Index.id))
    remote_id = Column(String)
    type = Column(String, nullable=False)
    title = Column(String)
    prompt = Column(String)
    value = Column(String)

    items = relationship(
        "InstrumentNode",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNode.order",
    )

