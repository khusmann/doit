import typing as t
from .sqlmodel import (
    CodeMapSql,
    ColumnEntrySql,
    InstrumentEntrySql,
    InstrumentNodeSql,
    MeasureEntrySql,
)

from ..view import (
    ConstantInstrumentNodeView,
    GroupMeasureNodeView,
    InstrumentNodeView,
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    OrdinalMeasureNodeView,
    SimpleColumnView,
    SimpleMeasureNodeView,
    OrdinalLabel,
    OrdinalValue,
    OrdinalTag,
)

def to_measureview(entry: MeasureEntrySql) -> MeasureView:
    return MeasureView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        items=tuple(
            to_measurenodeview(n) for n in entry.items
        ),
    )

def to_ordinalmeasurenodeview(entry: ColumnEntrySql) -> OrdinalMeasureNodeView:
    # TODO: Make types better?

    class CodeMapValue(t.TypedDict):
        value: OrdinalValue
        tag: OrdinalTag
        text: OrdinalLabel

    codemap: CodeMapSql | None = entry.codemap # type: ignore
    
    if not codemap:
        raise Exception("Error: {} missing codemap".format(entry.name))

    codemap_values: t.Sequence[CodeMapValue] = codemap.values  # type: ignore

    return OrdinalMeasureNodeView(
        name=entry.name,
        prompt=entry.prompt,
        tag_map={i['value']: i['tag'] for i in codemap_values},
        label_map={i['value']: i['text'] for i in codemap_values},
        type=entry.type, # type: ignore
        entity_type='ordinalmeasurenode',
    )

def to_simplemeasurenodeview(entry: ColumnEntrySql) -> SimpleMeasureNodeView:
    return SimpleMeasureNodeView(
        name=entry.name,
        prompt=entry.prompt,
        type=entry.type, # type: ignore
        entity_type='simplemeasurenode',
    )

def to_groupmeasurenodeview(entry: ColumnEntrySql) -> GroupMeasureNodeView:
    return GroupMeasureNodeView(
        name=entry.name,
        prompt=entry.prompt,
        items=tuple(
            to_measurenodeview(i) for i in entry.items
        ),
        entity_type='groupmeasurenode',
    )

VIEW_CONV_LOOKUP = {
    "ordinal": to_ordinalmeasurenodeview,
    "categorical": to_ordinalmeasurenodeview,
    "text": to_simplemeasurenodeview,
    "real": to_simplemeasurenodeview,
    "integer": to_simplemeasurenodeview,
    "group": to_groupmeasurenodeview,
}

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    view_conv = VIEW_CONV_LOOKUP.get(entry.type)
    if not view_conv:
        raise Exception("Error: No view conversion for type {}".format(entry.type))
    return view_conv(entry)


def to_instrumentnodeview(entry: InstrumentNodeSql) -> InstrumentNodeView:
    return ConstantInstrumentNodeView(
        value=entry.constant_value,
        entity_type="constantinstrumentnode",
    )

def to_instrumentview(entry: InstrumentEntrySql) -> InstrumentView:
    return InstrumentView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        instructions=entry.instructions,
        nodes=tuple(
            to_instrumentnodeview(i) for i in entry.items
        ),
    )

def to_columnview() -> ColumnView:
    return SimpleColumnView(
        name="stub.foo",
        prompt="stub",
        type="text",
        entity_type='simplecolumn'
    )

