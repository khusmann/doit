import typing as t
from pydantic import parse_obj_as

from ...common import ImmutableBaseModel

from ...common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

from .sqlmodel import (
    CodemapSql,
    ColumnEntrySql,
    InstrumentEntrySql,
    InstrumentNodeSql,
    MeasureEntrySql,
)

from ..view import (
    ConstantInstrumentNodeView,
    GroupInstrumentNodeView,
    GroupMeasureNodeView,
    InstrumentNodeView,
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    OrdinalColumnView,
    OrdinalMeasureNodeView,
    QuestionInstrumentNodeView,
    SimpleColumnView,
    SimpleMeasureNodeView,
    CodemapView,
    IndexColumnView,
)

def to_measureview(entry: MeasureEntrySql):
    return MeasureView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        items=tuple(
            to_measurenodeview(n) for n in entry.items
                if n.parent_column_id is None
        ),
    )

def to_codemapview(entry: CodemapSql) -> CodemapView:
    class CodemapValue(ImmutableBaseModel):
        value: OrdinalValue
        tag: OrdinalTag
        text: OrdinalLabel

    codemap_values = parse_obj_as(t.Tuple[CodemapValue, ...], entry.values)

    return CodemapView(
        tags={i.value: i.tag for i in codemap_values},
        labels={i.value: i.text for i in codemap_values},
    )

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    entry_type = entry.type
    match entry_type:
        case 'ordinal' | 'categorical':
            codemap_sql: CodemapSql | None = entry.codemap # type: ignore

            if not codemap_sql:
                raise Exception("Error: missing codemap")

            return OrdinalMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry_type,
                codes=to_codemapview(codemap_sql)
            )
        case 'text' | 'real' | 'integer':
            return SimpleMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry_type,
            )
        case 'group':
            return GroupMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                items=tuple(
                    to_measurenodeview(i) for i in entry.items
                ),
            )
        case _:
            raise Exception("Error: Unknown measure node type {}".format(entry.type))

def to_instrumentnodeview(entry: InstrumentNodeSql) -> InstrumentNodeView:
    entry_type = entry.type
    match entry_type:
        case 'question':
            return QuestionInstrumentNodeView(
                prompt=entry.prompt,
                source_column_name=entry.source_column_name,
                map={},
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None, # type: ignore
            )
        case 'constant':
            return ConstantInstrumentNodeView(
                value=entry.constant_value,
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None, # type: ignore
            )
        case 'group':
            return GroupInstrumentNodeView(
                title=entry.title,
                prompt=entry.prompt,
                items=tuple(
                    to_instrumentnodeview(i) for i in entry.items
                )
            )
        case _:
            raise Exception("Error: Unknown instrument node type {}".format(entry.type))

def to_instrumentview(entry: InstrumentEntrySql):
    return InstrumentView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        instructions=entry.instructions,
        nodes=tuple(
            to_instrumentnodeview(i) for i in entry.items
                if i.parent_node_id is None
        ),
    )

def to_columnview(entry: ColumnEntrySql) -> ColumnView:
    entry_type = entry.type
    match entry_type:
        case 'ordinal' | 'categorical' | 'index':
            codemap_sql: CodemapSql | None = entry.codemap # type: ignore

            if not codemap_sql:
                raise Exception("Error: missing codemap")

            if entry_type == 'index':
                return IndexColumnView(
                    name=entry.name,
                    title=entry.title,
                    codes=to_codemapview(codemap_sql)
                )
            else:
                return OrdinalColumnView(
                    name=entry.name,
                    prompt=entry.prompt,
                    type=entry_type,
                    codes=to_codemapview(codemap_sql)
                )

        case 'real' | 'text' | 'integer':
            return SimpleColumnView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry_type,
            )

        case _:
            raise Exception("Error: unknown column type {}".format(entry_type))

