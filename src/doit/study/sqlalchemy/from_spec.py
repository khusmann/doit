import typing as t
from .sqlmodel import (
    CodemapSql,
    MeasureEntrySql,
    ColumnEntrySql,
    InstrumentEntrySql,
    InstrumentNodeSql,
)

from ..spec import (
    CodeMapSpec,
    ConstantInstrumentItemSpec,
    IndexColumnSpec,
    InstrumentItemGroupSpec,
    InstrumentNodeSpec,
    InstrumentSpec,
    MeasureSpec,
    MeasureNodeSpec,
    OrdinalMeasureItemSpec,
    QuestionInstrumentItemSpec,
    RelativeCodeMapName,
    SimpleMeasureItemSpec,
    MultiselectItemSpec,
    MeasureItemGroupSpec,
)

def sql_from_codemap_spec(spec: CodeMapSpec, measure_name: str, codemap_name: str):
    name = ".".join((measure_name, codemap_name))
    return CodemapSql(
        name=name,
        values=spec.__root__,
    )

def sql_from_index_column_spec(spec: IndexColumnSpec, index_name: str):
    name = ".".join(('indices', index_name))
    return ColumnEntrySql(
        name=name,
        title=spec.title,
        description=spec.description,
        type='index',
        codemap=sql_from_codemap_spec(spec.values, "indices", index_name),
    )

class AddMeasureContext(t.NamedTuple):
    get_codemap_by_relname: t.Callable[[RelativeCodeMapName], CodemapSql]

    def sql_from_measure_spec(self, spec: MeasureSpec, name: str):
        return MeasureEntrySql(
            name=name,
            title=spec.title,
            description=spec.description,
            items=[
                self.sql_from_measurenode_spec(item, name) for item in spec.items
            ]
        )

    def sql_from_measurenode_spec(self, spec: MeasureNodeSpec, parent_name: str) -> ColumnEntrySql:
            name = ".".join((parent_name, spec.id))
            match spec:
                case OrdinalMeasureItemSpec():
                    codemap = self.get_codemap_by_relname(spec.codes)
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
                            self.sql_from_measurenode_spec(item, name) for item in spec.items
                        ]
                    )

class AddInstrumentContext(t.NamedTuple):
    get_column_by_name: t.Callable[[str], ColumnEntrySql]

    def sql_from_instrument_spec(self, spec: InstrumentSpec, name: str):
        return InstrumentEntrySql(
            name=name,
            title=spec.title,
            description=spec.description,
            instructions=spec.instructions,
            items=[
                self.sql_from_instrumentnode_spec(item)
                    for item in spec.items
            ]
        )

    def sql_from_instrumentnode_spec(self, spec: InstrumentNodeSpec):
        match spec:
            case QuestionInstrumentItemSpec():
                return InstrumentNodeSql(
                    prompt=spec.prompt,
                    source_column_name=spec.remote_id,
                    source_value_map={},
                    type=spec.type,
                    column_entry=None if spec.id is None else self.get_column_by_name(spec.id)
                )
            case ConstantInstrumentItemSpec():
                return InstrumentNodeSql(
                    constant_value=spec.value,
                    type=spec.type,
                    column_entry=None if spec.id is None else self.get_column_by_name(spec.id)
                )
            case InstrumentItemGroupSpec():
                return InstrumentNodeSql(
                    title=spec.title,
                    prompt=spec.prompt,
                    type=spec.type,
                    items=[
                        self.sql_from_instrumentnode_spec(i) for i in spec.items
                    ],
                )