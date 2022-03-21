# type: ignore
from __future__ import annotations
import typing as t
from pathlib import Path
from urllib.parse import urljoin
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    backref,
    joinedload,
    relationship,
    Session,
)

from sqlalchemy.orm.collections import attribute_mapped_collection

from ..domain.value import study

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

    def __init__(self, tag: str, parent: MeasureNode=None):
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
    def __init__(self, tag: str, parent: MeasureNode=None, title: str=""):
        super().__init__(tag, parent)
        self.title = title

    __mapper_args__ = {
        'polymorphic_identity':'group'
    }


def measure_to_sql(tag: str, node: study.MeasureNode, parent: t.Optional[MeasureNode] = None) -> t.Tuple[str, MeasureNode]:
    if node.type == 'group':
        new_node = GroupMeasureNode(tag, parent, node.prompt)
        new_children = [measure_to_sql(".".join([tag, t]), n, new_node) for (t, n) in node.items.items()]
    else:
        new_node = MeasureNode(tag, parent)
        new_children = []
    return [(tag, new_node), *sum(new_children, [])]


class StudyDbWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        url = "sqlite:///{}".format(path)
        print(url)
        self.engine = create_engine(url, echo=True)

        Base.metadata.create_all(self.engine)

    def add_measures(self, root: study.MeasureNode):
        sql = measure_to_sql("root", root)
        for (tag, item) in dict(sql).items():
            print(tag)
            print(item.dump())
        session = Session(self.engine)
        session.add(sql[0][1])
        session.commit()
