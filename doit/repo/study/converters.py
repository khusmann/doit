from __future__ import annotations

from ...domain.value.study import *
from .model import *

def entity_to_sql(entity: StudyEntity):
    match entity:
        case CodeMap():
            return CodeMapSql(
                id=entity.id,
                name=entity.name,
                values=entity.values,
            )
        case IndexColumn():
            return IndexColumnSql(
                id=entity.id,
                name=entity.name,
                codemap_id=entity.codemap_id,
            )
        case Measure():
            return MeasureSql(
                id=entity.id,
                name=entity.name,
                title=entity.title,
                description=entity.description,
            )
        case MeasureItemGroup():
            return MeasureNodeSql(
                id=entity.id,
                name=entity.name,
                parent_node_id=entity.parent_node_id,
                parent_measure_id=entity.parent_measure_id,
                prompt=entity.prompt,
                type=entity.type,
            )
        case OrdinalMeasureItem():
            return MeasureNodeSql(
                id=entity.id,
                name=entity.name,
                parent_node_id=entity.parent_node_id,
                parent_measure_id=entity.parent_measure_id,
                codemap_id=entity.codemap_id,
                prompt=entity.prompt,
                type=entity.type,
            )
        case SimpleMeasureItem():
            return MeasureNodeSql(
                id=entity.id,
                name=entity.name,
                parent_node_id=entity.parent_node_id,
                parent_measure_id=entity.parent_measure_id,
                studytable_id=entity.studytable_id,
                prompt=entity.prompt,
                type=entity.type,
            )
        case Instrument():
            return InstrumentSql(
                id=entity.id,
                name=entity.name,
                studytable_id=entity.studytable_id,
                title=entity.title,
                description=entity.description,
            )
        case QuestionInstrumentItem():
            return InstrumentNodeSql(
                id=entity.id,
                parent_node_id=entity.parent_node_id,
                parent_instrument_id=entity.parent_instrument_id,
                source_column_name=entity.source_column_name,
                measure_node_id=entity.measure_node_id,
                prompt=entity.prompt,
                type=entity.type,
            )
        case HiddenInstrumentItem():
            return InstrumentNodeSql(
                id=entity.id,
                parent_node_id=entity.parent_node_id,
                parent_instrument_id=entity.parent_instrument_id,
                source_column_name=entity.source_column_name,
                measure_node_id=entity.measure_node_id,
                type=entity.type,
            )
        case ConstantInstrumentItem():
            return InstrumentNodeSql(
                id=entity.id,
                parent_node_id=entity.parent_node_id,
                parent_instrument_id=entity.parent_instrument_id,
                measure_node_id=entity.measure_node_id,
                type=entity.type,
                value=entity.value,
            )
        case InstrumentItemGroup():
            return InstrumentNodeSql(
                id=entity.id,
                parent_node_id=entity.parent_node_id,
                parent_instrument_id=entity.parent_instrument_id,
                type=entity.type,
                prompt=entity.prompt,
                title=entity.title,
            )
        case StudyTable():
            pass

    raise Exception("{} not implemented yet".format(entity))