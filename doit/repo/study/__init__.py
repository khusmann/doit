from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from .model import *

from ...domain.value import studyspec

#def instrument_to_sql(instrument: studyspec.InstrumentSpec) -> InstrumentNode:
#    pass

def measure_to_sql(measure_spec: studyspec.MeasureSpec) -> t.Tuple[Measure, t.Mapping[str, MeasureNode]]:
    root = Measure(
        tag=measure_spec.measure_id,
        title=measure_spec.title,
        description=measure_spec.description,
    )

    codemaps = {
        str(tag): CodeMap(
            tag=tag,
            values=codemap.dict()['__root__'],
            root_measure=root,
        ) for (tag, codemap) in measure_spec.codes.items()
    }


    result: t.Mapping[str, MeasureNode] = {}
    for (tag, node) in measure_spec.items.items():
        result.update(
            measure_node_to_sql(
                tag=".".join([measure_spec.measure_id, tag]),
                node=node,
                parent=None,
                root=root,
                codemaps=codemaps,
            )
        )

    return (root, result)

def measure_node_to_sql(tag: str, node: studyspec.MeasureNode, parent: t.Optional[MeasureNode], root: Measure, codemaps: t.Mapping[str, CodeMap]) -> t.Mapping[str, MeasureNode]:
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
                result.update(measure_node_to_sql(".".join([tag, child_tag]), child_node, curr, root, codemaps))
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
                    codes=codemaps[node.codes],
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

    def add_study_spec(self, study_spec: studyspec.StudySpec):
        session = Session(self.engine)

        all_measure_items: t.Mapping[str, MeasureNode] = {}
        for (measure, measure_items) in map(measure_to_sql, study_spec.measures.values()):
            all_measure_items.update(measure_items)
            session.add(measure) # type: ignore


        session.commit()
