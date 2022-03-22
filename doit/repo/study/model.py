# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    backref,
    relationship,
)

from sqlalchemy.orm.collections import attribute_mapped_collection

Base = declarative_base()

# Turn into ABC?
class MeasureNode(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey(id))
    tag = Column(String, nullable=False, unique=True)
    type = Column(String)
    items = relationship(
        "MeasureNode", backref=backref("parent", remote_side=id), collection_class=attribute_mapped_collection("tag"),
    )

    def __init__(self, tag: str, parent: t.Optional[MeasureNode]=None):
        self.tag = tag
        self.parent = parent

    def __repr__(self):
        return "MeasureNode(tag={})".format(
            self.tag,
        )

    def dump(self, _indent=0):
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.items.values()])
        )
    __mapper_args__ = {
        'polymorphic_on':type
    }

class GroupMeasureNode(MeasureNode):
    title = Column(String)
    def __init__(self, tag: str, parent: t.Optional[MeasureNode]=None, title: str=""):
        super().__init__(tag, parent)
        self.title = title

    __mapper_args__ = {
        'polymorphic_identity':'group'
    }

