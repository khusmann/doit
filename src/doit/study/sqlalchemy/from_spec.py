import typing as t
from .sqlmodel import (
    CodeMapSql,
    MeasureEntrySql,
    ColumnEntrySql
)

from ..spec import (
    CodeMapSpec,
    MeasureSpec,
    MeasureNodeSpec,
    OrdinalMeasureItemSpec,
    RelativeCodeMapName,
    SimpleMeasureItemSpec,
    MultiselectItemSpec,
    MeasureItemGroupSpec,
)

def sql_from_measure_spec(spec: MeasureSpec, name: str):
    codemaps = {
        cm_name: sql_from_codemap_spec(cm, name, cm_name)
            for cm_name, cm in spec.codes.items()
    }

    return MeasureEntrySql(
        name=name,
        title=spec.title,
        description=spec.description,
        items=[
            sql_from_measurenode_spec(lambda x: codemaps.get(x))(item, name) for item in spec.items
        ]
    )

def sql_from_codemap_spec(spec: CodeMapSpec, measure_name: str, codemap_name: str):
    name = ".".join((measure_name, codemap_name))
    return CodeMapSql(
        name=name,
        values=spec.__root__,
    )

def sql_from_measurenode_spec(codemap_by_relname: t.Callable[[RelativeCodeMapName], CodeMapSql | None]):
    def inner(spec: MeasureNodeSpec, parent_name: str) -> ColumnEntrySql:
        name = ".".join((parent_name, spec.id))

        match spec:
            case OrdinalMeasureItemSpec():
                codemap = codemap_by_relname(spec.codes)
                if not codemap:
                    raise Exception("Error: Cannot find codemap: {}".format(spec.codes))
                return ColumnEntrySql(
                    name=name,
                    type=spec.type,
                    prompt=spec.prompt,
                    codemap=codemap,
                )
            case SimpleMeasureItemSpec():
                return ColumnEntrySql(
                    name=name,
                    prompt=spec.prompt,
                    type=spec.type,
                )
            case MultiselectItemSpec():
                raise Exception("Error: MultiselectItemSpec not implemented")
            case MeasureItemGroupSpec():
                return ColumnEntrySql(
                    name=name,
                    type=spec.type,
                    prompt=spec.prompt,
                    items=[
                        inner(item, name) for item in spec.items
                    ]
                )
    return inner


