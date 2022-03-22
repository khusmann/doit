# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
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
        backref="root_measure",
        collection_class=attribute_mapped_collection("tag"),
    )

class MeasureNode(Base):
    __tablename__ = "__measure_items__"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey(id))
    root_measure_id = Column(Integer, ForeignKey(Measure.id))
    codes = Column(String)
    is_idx = Column(Boolean)
    prompt = Column(String)
    tag = Column(String, nullable=False, unique=True)
    type = Column(String)
    items = relationship(
        "MeasureNode",
        backref=backref("parent", remote_side=id),
        collection_class=attribute_mapped_collection("tag"),
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