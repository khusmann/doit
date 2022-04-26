from .sqlmodel import (
    MeasureEntrySql,
    ColumnEntrySql
)

from ..spec import (
    MeasureSpec,
    MeasureNodeSpec,
    OrdinalMeasureItemSpec,
    SimpleMeasureItemSpec,
    MultiselectItemSpec,
    MeasureItemGroupSpec,
)

def sql_from_measure_spec(spec: MeasureSpec, name: str):
    return MeasureEntrySql(
        name=name,
        title=spec.title,
        description=spec.description,
        items=[
            sql_from_measurenode_spec(item, name) for item in spec.items
        ]
    )

def sql_from_measurenode_spec(spec: MeasureNodeSpec, parent_name: str) -> ColumnEntrySql:
    name = ".".join((parent_name, spec.id))

    match spec:
        case OrdinalMeasureItemSpec():
            return ColumnEntrySql(
                name=name,
                type=spec.type,
                prompt=spec.prompt,
            )
        case SimpleMeasureItemSpec():
            return ColumnEntrySql(
                name=name,
                type=spec.type,
            )
        case MultiselectItemSpec():
            raise Exception("Error: MultiselectItemSpec implemented")
        case MeasureItemGroupSpec():
            return ColumnEntrySql(
                name=name,
                type=spec.type,
                prompt=spec.prompt,
                items=[
                    sql_from_measurenode_spec(item, name) for item in spec.items
                ]
            )


