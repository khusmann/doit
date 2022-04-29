import typing as t

from ..common.table import (
    IncorrectTypeError,
    Omitted,
    Redacted,
    ErrorValue,
    TableValue,
    Some,
)

from ..linker.model import (
    Linker,
    InstrumentLinker,
)

from ..study.view import (
    DstLink,
    InstrumentLinkerSpec,
    LinkerSpec,
    OrdinalDstLink,
    SimpleDstLink,
    SrcLink,
    ConstantSrcLink,
    QuestionSrcLink,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedTableRowView,
    LinkedTableData,
    LinkedColumnId,
)

def from_src(
    column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo],
    src: SrcLink,
    row: SanitizedTableRowView,
):
    match src:
        case QuestionSrcLink():
            src_name = SanitizedColumnId(src.source_column_name)
            column_info = column_lookup.get(src_name)
            if not column_info:
                raise Exception("Error: cannot find column in instrument source table ({})".format(src.source_column_name))
            match column_info:
                case SanitizedColumnInfo(): # Ordinal? Multiselect?
                    return row.get(src_name)
        case ConstantSrcLink():
            return Some(src.constant_value)

def to_dst(dst: DstLink, tv: TableValue):
    linked_name = LinkedColumnId(dst.linked_name)
    match dst:
        case OrdinalDstLink():
            match tv:
                case Some(value=value) if isinstance(value, str):
                    #return ((linked_name, Some(dst.value_from_tag[value])),)
                    return ((linked_name, Some(1)),)
                case Some(value=value):
                    return ((linked_name, ErrorValue(IncorrectTypeError(value))),)
                case Omitted() | Redacted() | ErrorValue():
                    return ((linked_name, tv),)
        case SimpleDstLink():
            return ((linked_name, tv),)

def linker_from_spec(column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo], spec: LinkerSpec):
    return Linker(
        dst_col_ids=(LinkedColumnId(spec.dst.linked_name),),
        link_fn=lambda row: to_dst(spec.dst, from_src(column_lookup, spec.src, row)),
    )

def link_tableinfo(tableinfo: SanitizedTableInfo, instrumentlinker_spec: InstrumentLinkerSpec) -> InstrumentLinker:
    column_lookup = { c.id: c for c in tableinfo.columns }
    return InstrumentLinker(
        studytable_name=instrumentlinker_spec.studytable_name,
        instrument_name=instrumentlinker_spec.instrument_name,
        linkers=tuple(
            linker_from_spec(column_lookup, rowlinker_spec)
                for rowlinker_spec in instrumentlinker_spec.linker_specs
        ),
    )

def link_tabledata(table: SanitizedTableData, instrument_linker: InstrumentLinker) -> LinkedTableData:
    dst_column_ids = tuple(
        i
            for row_linker in instrument_linker.linkers
                for i in row_linker.dst_col_ids
    )

    rows = tuple(
        LinkedTableRowView(dict(
            i
                for row_linker in instrument_linker.linkers
                    for i in row_linker.link_fn(row) 
        )) for row in table.rows
    )

    return LinkedTableData(
        column_ids=dst_column_ids,
        rows=rows,
    )
