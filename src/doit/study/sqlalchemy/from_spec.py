import typing as t
from itertools import count

from ..model import LinkedTable, LinkedColumnInfo

from ...common.table import (
    ColumnNotFoundInRow,
    IncorrectType,
    Some,
    Omitted,
    Redacted,
    ErrorValue,
    TableValue,
    TableErrorReport,
    TableErrorReportItem,
)

from .sqlmodel import (
    CodemapSql,
    ColumnEntryType,
    InstrumentNodeType,
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
    CodedMeasureItemSpec,
    QuestionInstrumentItemSpec,
    RelativeCodeMapName,
    SimpleMeasureItemSpec,
    MeasureItemGroupSpec,
)

def sql_from_tablevalue(column: LinkedColumnInfo, value: TableValue[t.Any]):
    match column.value_type:
        case 'text':
            tv = value.assert_type(str)
            match tv:
                case Some(value=v):
                    return v
                case Omitted():
                    return None
                case Redacted():
                    return "__REDACTED__"
                case ErrorValue():
                    return tv

        case  'real' | 'integer':
            tv = value.assert_type(str)
            match tv:
                case Some(value=v):
                    return v
                case Omitted():
                    return None
                case Redacted():
                    return ErrorValue(IncorrectType(value))
                case ErrorValue():
                    return tv

        case 'ordinal' | 'categorical':
            tv = value.assert_type(int)
            match tv:
                case Some(value=v):
                    return v
                case Omitted():
                    return None
                case Redacted():
                    return ErrorValue(IncorrectType(value))
                case ErrorValue():
                    return tv

        case 'index':
            tv = value.assert_type(int)
            match tv:
                case Some(value=v):
                    return v
                case Omitted():
                    return ErrorValue(IncorrectType(value))
                case Redacted():
                    return ErrorValue(IncorrectType(value))
                case ErrorValue():
                    return tv

        case 'multiselect':
            tv = value.assert_type_seq(int)
            match tv:
                case Some(value=v):
                    return v
                case Omitted():
                    return None
                case Redacted():
                    return ErrorValue(IncorrectType(value))
                case ErrorValue():
                    return tv


def render_tabledata(linked_table: LinkedTable):
    errors: TableErrorReport = set()

    def filter_error(column: LinkedColumnInfo, value: TableValue[t.Any]):
        linked_name = column.id.linked_name
        filtered_value = sql_from_tablevalue(column, value)
        if isinstance(filtered_value, ErrorValue):
            errors.add(TableErrorReportItem(linked_table.instrument_name, linked_name, filtered_value))
            return None
        else:
            return filtered_value

    rendered_values = tuple(
        { c.id.linked_name: filter_error(c, row.get(c.id)) for c in linked_table.columns }
            for row in linked_table.data.rows
    )

    index_names = tuple(i.id.linked_name for i in linked_table.columns if i.value_type == 'index')

    def is_valid_row(row: t.Mapping[str, t.Any]):
        missing_index = next((i for i in index_names if row[i] is None), None)
        if missing_index:
            errors.add(TableErrorReportItem(linked_table.instrument_name, missing_index, ErrorValue(ColumnNotFoundInRow(missing_index, row))))
            return False
        else:
            return True

    return tuple(row for row in rendered_values if is_valid_row(row)), errors

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
        shortname=index_name,
        title=spec.title,
        description=spec.description,
        type=ColumnEntryType.INDEX,
        codemap=sql_from_codemap_spec(spec.values, "indices", index_name),
    )

def sql_columnentrytype(spec: MeasureNodeSpec) -> ColumnEntryType:
    match spec.type:
        case 'group':
            return ColumnEntryType.GROUP
        case 'real':
            return ColumnEntryType.REAL
        case 'categorical':
            return ColumnEntryType.CATEGORICAL
        case 'ordinal':
            return ColumnEntryType.ORDINAL
        case 'integer':
            return ColumnEntryType.INTEGER
        case 'text':
            return ColumnEntryType.TEXT
        case 'multiselect':
            return ColumnEntryType.MULTISELECT

class AddMeasureContext(t.NamedTuple):
    get_codemap_by_relname: t.Callable[[RelativeCodeMapName], CodemapSql]

    def sql_from_measure_spec(self, spec: MeasureSpec, name: str):
        return MeasureEntrySql(
            name=name,
            title=spec.title,
            description=spec.description,
            items=self.sql_from_measurenode_spec(spec.items, name),
        )

    def sql_from_measurenode_spec(self, specs: t.Sequence[MeasureNodeSpec], parent_name: str) -> t.List[ColumnEntrySql]:
        def inner(spec: MeasureNodeSpec) -> t.List[ColumnEntrySql]:
                name = ".".join((parent_name, spec.id))
                match spec:
                    case CodedMeasureItemSpec():
                        codemap = self.get_codemap_by_relname(spec.codes)
                        if not codemap:
                            raise Exception("Error: Cannot find codemap: {}".format(spec.codes))
                        return [ColumnEntrySql(
                            name=name,
                            type=sql_columnentrytype(spec),
                            prompt=spec.prompt,
                            codemap=codemap,
                        )]
                    case SimpleMeasureItemSpec():
                        return [ColumnEntrySql(
                            name=name,
                            prompt=spec.prompt,
                            type=sql_columnentrytype(spec),
                        )]
                    case MeasureItemGroupSpec():
                        items=self.sql_from_measurenode_spec(spec.items, name)
                        return [ColumnEntrySql(
                            name=name,
                            type=sql_columnentrytype(spec),
                            prompt=spec.prompt,
                            items=items,
                        ), *items]
        return [
            sql
                for i in specs
                    for sql in inner(i)
        ]

def sql_instrumentnodetype(spec: InstrumentNodeSpec) -> InstrumentNodeType:
    match spec.type:
        case 'question':
            return InstrumentNodeType.QUESTION
        case 'constant':
            return InstrumentNodeType.CONSTANT
        case 'group':
            return InstrumentNodeType.GROUP

class AddInstrumentContext(t.NamedTuple):
    get_column_by_name: t.Callable[[str], ColumnEntrySql]
    sortkey = count(0)

    def sql_from_instrument_spec(self, spec: InstrumentSpec, name: str):
        return InstrumentEntrySql(
            name=name,
            title=spec.title,
            description=spec.description,
            instructions=spec.instructions,
            items=self.sql_from_instrumentnode_spec(spec.items),
        )

    def sql_from_instrumentnode_spec(self, specs: t.Sequence[InstrumentNodeSpec]) -> t.List[InstrumentNodeSql]:
        def inner(spec: InstrumentNodeSpec) -> t.List[InstrumentNodeSql]:
            match spec:
                case QuestionInstrumentItemSpec():
                    return [InstrumentNodeSql(
                        prompt=spec.prompt,
                        source_column_name=spec.remote_id,
                        source_value_map=spec.map,
                        type=sql_instrumentnodetype(spec),
                        column_entry=None if spec.id is None else self.get_column_by_name(spec.id),
                        sortkey=next(self.sortkey),
                    )]
                case ConstantInstrumentItemSpec():
                    return [InstrumentNodeSql(
                        constant_value=spec.value,
                        type=sql_instrumentnodetype(spec),
                        column_entry=None if spec.id is None else self.get_column_by_name(spec.id),
                        sortkey=next(self.sortkey),
                    )]
                case InstrumentItemGroupSpec():
                    curr_sortkey = next(self.sortkey)
                    items = self.sql_from_instrumentnode_spec(spec.items)
                    return [InstrumentNodeSql(
                        title=spec.title,
                        prompt=spec.prompt,
                        type=sql_instrumentnodetype(spec),
                        items=items,
                        sortkey=curr_sortkey,
                    ), *items]
        return [
            sql
                for i in specs
                    for sql in inner(i)
        ]