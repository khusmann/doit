from __future__ import annotations
import typing as t

from ...domain.value.common import CodeMapTag, MeasureId, merge_mappings

from .model import *

from ...domain.value import studyspec
from ...domain.value.common import MeasureNodeTag

from itertools import starmap

def seq_instrumentspec_to_sql(instrumentspecs: t.Sequence[studyspec.InstrumentSpec]) -> t.Tuple[Instrument, ...]:
    return tuple((
        Instrument(
            tag=spec.instrument_id,
            title=spec.title,
            description=spec.description,
            instructions=spec.instructions,
        ) for spec in instrumentspecs
    ))

def instrument_node_tree_to_sql(node_tree: t.Sequence[studyspec.InstrumentNode], parent: InstrumentNode | Instrument, measures: t.Mapping[str, MeasureNode]) -> t.Tuple[InstrumentNode, ...]:
    def branch(node: studyspec.InstrumentNode) -> t.Tuple[InstrumentNode, ...]:
        new_node = instrument_node_to_sql(
            node=node,
            parent=parent,
            measures=measures,
        )
        children = instrument_node_tree_to_sql(
            node_tree=node.items,
            parent=new_node,
            measures=measures,
        ) if node.type == 'group' else tuple()
        return (new_node,) + children

    return sum(map(branch, node_tree), tuple())

def instrument_node_to_sql(node: studyspec.InstrumentNode, parent: InstrumentNode | Instrument, measures: t.Mapping[str, MeasureNode]) -> InstrumentNode:
    parent_node = parent if isinstance(parent, InstrumentNode) else None
    parent_instrument = parent if isinstance(parent, Instrument) else None
    match node:
        case studyspec.QuestionInstrumentItem():
            print(node.id)
            print(measures.get(str(node.id)))
            print(list(measures.keys()))
            return InstrumentNode(
                parent_node=parent_node,
                parent_instrument=parent_instrument,
                measure_item=measures.get(str(node.id)),
                remote_id=node.remote_id,
                type=node.type,
                prompt=node.prompt,
            )
        case studyspec.ConstantInstrumentItem():
            return InstrumentNode(
                parent_node=parent_node,
                parent_instrument=parent_instrument,
                measure_item=measures.get(str(node.id)),
                type=node.type,
                value=node.value,
            )

        case studyspec.HiddenInstrumentItem():
            return InstrumentNode(
                parent_node=parent_node,
                parent_instrument=parent_instrument,
                measure_item=measures.get(str(node.id)),
                remote_id=node.remote_id,
                type=node.type,
            )

        case studyspec.InstrumentItemGroup():
            return InstrumentNode(
                parent_node=parent_node,
                parent_instrument=parent_instrument,
                type=node.type,
                prompt=node.prompt,
                title=node.title,
            )

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
            __root__=codemap.dict()['__root__'],
            parent_measure=parent,
        ) for (tag, codemap) in codemap_map.items()
    }

def measure_node_tree_to_sql(node_map: t.Mapping[MeasureNodeTag, studyspec.MeasureNode], parent: MeasureNode | Measure, codemaps: t.Mapping[str, CodeMap]) -> t.Dict[str, MeasureNode]:
    def branch(tag: MeasureNodeTag, node: studyspec.MeasureNode) -> t.Dict[str, MeasureNode]:
        full_tag = ".".join([str(parent.tag), str(tag)])
        new_node = measure_node_to_sql(
            tag=full_tag,
            node=node,
            parent=parent,
            codemaps=codemaps,
        )
        children = measure_node_tree_to_sql(
            node_map=node.items,
            parent=new_node,
            codemaps=codemaps,
        ) if node.type == 'group' else {}
        return { full_tag: new_node, **children }

    return merge_mappings(tuple(starmap(branch, node_map.items())))

def measure_node_to_sql(tag: str, node: studyspec.MeasureNode, parent: MeasureNode | Measure, codemaps: t.Mapping[str, CodeMap]) -> MeasureNode:
    parent_node = parent if isinstance(parent, MeasureNode) else None
    parent_measure = parent if isinstance(parent, Measure) else None 
    match node:
        case studyspec.MeasureItemGroup():
            return MeasureNode(
                tag=tag,
                prompt=node.prompt,
                parent_node=parent_node,
                type=node.type,
                parent_measure=parent_measure,
            )
        case studyspec.SimpleMeasureItem():
            return MeasureNode(
                tag=tag,
                prompt=node.prompt,
                parent_node=parent_node,
                type=node.type,
                parent_measure=parent_measure,
            )
        case studyspec.OrdinalMeasureItem():
            return MeasureNode(
                tag=tag,
                prompt=node.prompt,
                parent_node=parent_node,
                type=node.type,
                parent_measure=parent_measure,
                codes=codemaps.get(node.codes),
                is_idx=node.is_idx,
            )
