from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from ...domain.value.common import CodeMapTag, MeasureId, merge_mappings

from .model import *

from ...domain.value import studyspec
from ...domain.value.common import MeasureNodeTag

from itertools import starmap

#def instrument_to_sql(instrument: studyspec.InstrumentSpec) -> InstrumentNode:
#    pass

def mapped_measurespec_to_sql(measurespec_map: t.Mapping[MeasureId, studyspec.MeasureSpec]) -> t.Dict[str, Measure]:
    return {
        str(measure_id): Measure(
            tag=measure_spec.measure_id,
            title=measure_spec.title,
            description=measure_spec.description,
        ) for (measure_id, measure_spec) in measurespec_map.items()
    }

def mapped_codemaps_to_sql(codemap_map: t.Mapping[CodeMapTag, studyspec.CodeMap], parent: Measure) -> t.Dict[str, CodeMap]:
    return {
        str(tag): CodeMap(
            tag=tag,
            values=codemap.dict()['__root__'],
            root_measure=parent,
        ) for (tag, codemap) in codemap_map.items()
    }

def mapped_measure_node_to_sql(node_map: t.Mapping[MeasureNodeTag, studyspec.MeasureNode], parent: MeasureNode | Measure, codemaps: t.Mapping[str, CodeMap]) -> t.Dict[str, MeasureNode]:
    return merge_mappings([
        measure_node_to_sql(
            tag=".".join([str(parent.tag), str(tag)]),
            node=node,
            parent=parent,
            codemaps=codemaps,
        ) for (tag, node) in node_map.items()
    ])

def measure_node_to_sql(tag: str, node: studyspec.MeasureNode, parent: MeasureNode | Measure, codemaps: t.Mapping[str, CodeMap]) -> t.Dict[str, MeasureNode]:
    parent_node = parent if isinstance(parent, MeasureNode) else None
    parent_measure = parent if isinstance(parent, Measure) else None 
    match node:
        case studyspec.MeasureItemGroup():
            curr = MeasureNode(
                tag=tag,
                prompt=node.prompt,
                parent=parent_node,
                type=node.type,
                root_measure=parent_measure,
            )

            children = mapped_measure_node_to_sql(
                node_map=node.items,
                parent=curr,
                codemaps=codemaps,
            )

            return { str(tag): curr, **children }
        case studyspec.SimpleMeasureItem():
            return {
                tag: MeasureNode(
                    tag=tag,
                    prompt=node.prompt,
                    parent=parent_node,
                    type=node.type,
                    root_measure=parent_measure,
                )
            } 
        case studyspec.OrdinalMeasureItem():
            return {
                tag: MeasureNode(
                    tag=tag,
                    prompt=node.prompt,
                    parent=parent_node,
                    type=node.type,
                    codes=codemaps[node.codes],
                    is_idx=node.is_idx,
                    root_measure=parent_measure,
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

        sql_measures = mapped_measurespec_to_sql(study_spec.measures)

        codemap_specs = tuple(map(lambda m: m.codes, study_spec.measures.values()))

        sql_codemaps = tuple(starmap(mapped_codemaps_to_sql, zip(codemap_specs, sql_measures.values())))

        node_specs = tuple(map(lambda m: m.items, study_spec.measures.values()))

        sql_measure_nodes = merge_mappings(
            tuple(starmap(mapped_measure_node_to_sql, zip(node_specs, sql_measures.values(), sql_codemaps)))
        )

        print(sql_measure_nodes)

        for measure in sql_measures.values():
            session.add(measure) # type: ignore


        session.commit()
