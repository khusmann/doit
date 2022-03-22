from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from .model import *

from ...domain.value import studyspec

def measure_to_sql(measure: studyspec.MeasureSpec) -> t.Mapping[str, MeasureNode]:
    root = MeasureNode(
        tag=measure.measure_id,
        title=measure.title,
        description=measure.description,
        type='root',
    )

    result = { str(measure.measure_id): root }
    for (tag, node) in measure.items.items():
        result.update(measure_node_to_sql(".".join([measure.measure_id, tag]), node, root))
    return result

def measure_node_to_sql(tag: str, node: studyspec.MeasureNode, parent: MeasureNode) -> t.Mapping[str, MeasureNode]:
    match node:
        case studyspec.MeasureItemGroup():
            root = MeasureNode(
                tag=tag,
                prompt=node.prompt,
                type=node.type,
            )
            result = { str(tag): root }
            for (child_tag, child_node) in node.items.items():
                result.update(measure_node_to_sql(".".join([tag, child_tag]), child_node, root))
            return result
        case studyspec.SimpleMeasureItem():
            return {
                tag: MeasureNode(
                    tag=tag,
                    prompt=node.prompt,
                    type=node.type,
                )
            } 
        case studyspec.OrdinalMeasureItem():
            return {
                tag: MeasureNode(
                    tag=tag,
                    prompt=node.prompt,
                    type=node.type,
                    codes=node.codes,
                    is_idx=node.is_idx,
                )
            } 

class StudyRepoWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        url = "sqlite:///{}".format(path)
        print(url)
        self.engine = create_engine(url, echo=True)

        Base.metadata.create_all(self.engine)

    def add_measure_spec(self, measure_spec: studyspec.MeasureSpec):
        sql = measure_to_sql(measure_spec)
        session = Session(self.engine)
        for (k, v) in sql.items():
            print("{}: {}".format(k, v))
            session.add(v) # type: ignore
        session.commit()
