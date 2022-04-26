from .sqlmodel import (
    ColumnEntrySql,
    MeasureEntrySql,
)

from ..view import (
    GroupMeasureNodeView,
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    OrdinalMeasureNodeView,
    TextColumnView,
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

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    match entry.type:
        case "ordinal":
            return OrdinalMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                tag_map={},
                label_map={},
            )
        case "categorical":
            return OrdinalMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                tag_map={},
                label_map={},
            )
        case "group":
            return GroupMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                items=tuple(
                    to_measurenodeview(i) for i in entry.items
                )
            )
        case _:
            return OrdinalMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                tag_map={},
                label_map={},
            )           


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

