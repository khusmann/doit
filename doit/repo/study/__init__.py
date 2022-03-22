from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from .model import *

from ...domain.value import studyspec

#def instrument_to_sql(instrument: studyspec.InstrumentSpec) -> InstrumentNode:
#    pass

def measure_to_sql(measure: studyspec.MeasureSpec) -> t.Mapping[str, MeasureNode]:
    root = Measure(
        tag=measure.measure_id,
        title=measure.title,
        description=measure.description,
    )

    result = { str(measure.measure_id): root }
    for (tag, node) in measure.items.items():
        result.update(measure_node_to_sql(".".join([measure.measure_id, tag]), node, None, root))
    return result

def measure_node_to_sql(tag: str, node: studyspec.MeasureNode, parent: t.Optional[MeasureNode], root: Measure) -> t.Mapping[str, MeasureNode]:
    match node:
        case studyspec.MeasureItemGroup():
            curr = MeasureNode(
                tag=tag,
                prompt=node.prompt,
                parent=parent,
                type=node.type,
                root_measure=root,
            )
            result = { str(tag): root }
            for (child_tag, child_node) in node.items.items():
                result.update(measure_node_to_sql(".".join([tag, child_tag]), child_node, curr, root))
            return result
        case studyspec.SimpleMeasureItem():
            return {
                tag: MeasureNode(
                    tag=tag,
                    prompt=node.prompt,
                    parent=parent,
                    type=node.type,
                    root_measure=root,
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
                    root_measure=root,
                    parent=parent,
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
