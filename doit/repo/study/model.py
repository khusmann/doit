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

from sqlalchemy.orm.collections import attribute_mapped_collection

Base = declarative_base()

class Measure(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    tag = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items = relationship(
        "MeasureNode",
        backref="parent_measure",
        collection_class=attribute_mapped_collection("tag"),
    )
    codes = relationship(
        "CodeMap",
        backref="parent_measure",
        collection_class=attribute_mapped_collection("tag"),
    )

class CodeMap(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    parent_measure_id = Column(Integer, ForeignKey(Measure.id))
    tag = Column(String, nullable=False)
    __root__ = Column(JSON, nullable=False)
    measure_nodes = relationship(
        "MeasureNode",
        backref="codes",
        collection_class=attribute_mapped_collection("tag"),
    )

class MeasureNode(Base):
    __tablename__ = "__measure_items__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_measure_id = Column(Integer, ForeignKey(Measure.id))
    codemap_id = Column(Integer, ForeignKey(CodeMap.id))
    is_idx = Column(Boolean)
    prompt = Column(String)
    tag = Column(String, nullable=False, unique=True)
    type = Column(String, nullable=False)
    items = relationship(
        "MeasureNode",
        backref=backref("parent_node", remote_side=id),
        collection_class=attribute_mapped_collection("tag"),
    )

    instrument_items = relationship(
        "InstrumentNode",
        backref=backref("measure_item", remote_side=id),
    )


    def __repr__(self):
        return "MeasureNode(type={}, tag={})".format(
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
    )

class InstrumentNode(Base):
    __tablename__ = "__instrument_items__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_instrument_id = Column(Integer, ForeignKey(Instrument.id))
    measure_item_id = Column(Integer, ForeignKey(MeasureNode.id))
    remote_id = Column(String)
    type = Column(String, nullable=False)
    title = Column(String)
    prompt = Column(String)
    value = Column(String)

    items = relationship(
        "InstrumentNode",
        backref=backref("parent_node", remote_side=id),
    )
