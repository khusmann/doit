from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from .model import *

from ...domain.value import studyspec


def measure_to_sql(tag: str, node: studyspec.MeasureNode, parent: t.Optional[MeasureNode] = None) -> t.Sequence[t.Tuple[str, MeasureNode]]:
    if node.type == 'group':
        new_node = GroupMeasureNode(tag, parent, node.prompt)
        new_children = [measure_to_sql(".".join([tag, t]), n, new_node) for (t, n) in node.items.items()]
    else:
        new_node = MeasureNode(tag, parent)
        new_children: t.Sequence[t.Sequence[t.Tuple[str, MeasureNode]]] = []
    return [(tag, new_node), *sum(new_children, [])]

class StudyRepoWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        url = "sqlite:///{}".format(path)
        print(url)
        self.engine = create_engine(url, echo=True)

        Base.metadata.create_all(self.engine)

    def add_measures(self, root: studyspec.MeasureNode):
        sql = measure_to_sql("root", root)
        for (tag, item) in dict(sql).items():
            print(tag)
            print(item.dump())
        session = Session(self.engine)
        session.add(sql[0][1]) # type: ignore
        session.commit()
