from ...common.sqlalchemy import (
    str_or_none,
)

from .model import (
    ColumnEntrySql,
    MeasureEntrySql,
)

from ..view import (
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    OrdinalMeasureNodeView,
    TextColumnView,
)

def to_measureview(entry: MeasureEntrySql) -> MeasureView:
    return MeasureView(
        name=str(entry.name),
        title=str(entry.title),
        description=str_or_none(entry.description),
        items=tuple(
            to_measurenodeview(n) for n in entry.items
        ),
    )

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    match entry.type:
        case "ordinal":
            return OrdinalMeasureNodeView(
                name=str(entry.name),
                prompt=str(entry.prompt),
                tag_map={},
                label_map={},
            )
        case _:
            raise Exception("Not implemented")


def to_instrumentview() -> InstrumentView:
    return InstrumentView(
        name="stub",
        title="stub",
        description=None,
        instructions=None,
        nodes=(),
    )

def to_columnview() -> ColumnView:
    return TextColumnView(
        name="stub.foo",
        prompt="stub",
    )

