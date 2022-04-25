from .model import (
    ColumnEntrySql,
    MeasureEntrySql,
)

from ..view import (
    InstrumentView,
    MeasureNodeView,
    MeasureView,
    OrdinalMeasureNodeView,
    MeasureNodeName,
    MeasureName,
    InstrumentName,
    ColumnView,
    TextColumnView,
    ColumnName,
)

def to_measureview(entry: MeasureEntrySql) -> MeasureView:
    return MeasureView(
        name=MeasureName(str(entry.name)),
        title=str(entry.title),
        description=str(entry.description),
        items=tuple(
            to_measurenodeview(n) for n in entry.items
        ),
    )

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    match entry.type:
        case "ordinal":
            return OrdinalMeasureNodeView(
                name=MeasureNodeName(str(entry.name)),
                prompt=str(entry.prompt),
                tag_map={},
                label_map={},
            )
        case _:
            raise Exception("Not implemented")


def to_instrumentview() -> InstrumentView:
    return InstrumentView(
        name=InstrumentName("stub"),
        title="stub",
        description=None,
        instructions=None,
        nodes=(),
    )

def to_columnview() -> ColumnView:
    return TextColumnView(
        name=ColumnName("stub.foo"),
        prompt="stub",
    )

