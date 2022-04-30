import typing as t

from ..common.table import (
    IncorrectType,
    MissingCode,
    Omitted,
    Redacted,
    ErrorValue,
    TableValue,
    Some,
    OrdinalValue,
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
    SanitizedMultiselectColumnInfo,
    SanitizedOrdinalColumnInfo,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRowView,
    SanitizedTextColumnInfo,
)

from ..study.model import (
    LinkedTableRowView,
    LinkedTableData,
    LinkedTable,
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
                case SanitizedTextColumnInfo():
                    return row.get(src_name)
                case SanitizedOrdinalColumnInfo():
                    tv = row.get(src_name)
                    match tv:
                        case Some(value=value) if isinstance(value, int):
                            label = column_info.codes.get(OrdinalValue(value))
                            if label is None:
                                return ErrorValue(MissingCode(value, column_info.codes))
                            return Some(src.source_value_map.get(label, label))
                        case Some(value=value):
                            return ErrorValue(IncorrectType(value))
                        case _:
                            return tv
                case SanitizedMultiselectColumnInfo():
                    raise Exception("Error: multiselect not implenented yet")
        case ConstantSrcLink():
            return Some(src.constant_value)

def to_dst(dst: DstLink, tv: TableValue):
    linked_name = LinkedColumnId(dst.linked_name)
    match dst:
        case OrdinalDstLink():
            match tv:
                case Some(value=value) if isinstance(value, str):
                    int_value = dst.value_from_tag.get(value)
                    if int_value is None:
                        return LinkedTableRowView({ linked_name: ErrorValue(MissingCode(value, dst.value_from_tag)) })
                    return LinkedTableRowView({ linked_name: Some(int_value) })
                case Some(value=value):
                    return LinkedTableRowView({ linked_name: ErrorValue(IncorrectType(value)) })
                case Omitted() | Redacted() | ErrorValue():
                    return LinkedTableRowView({ linked_name: tv })
        case SimpleDstLink():
            return LinkedTableRowView({ linked_name: tv })

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

def link_table(table: SanitizedTableData, instrument_linker: InstrumentLinker) -> LinkedTable:
    dst_column_ids = tuple(
        i
            for row_linker in instrument_linker.linkers
                for i in row_linker.dst_col_ids
    )

    rows = tuple(
        LinkedTableRowView.combine_views(
            *(linker.link_fn(row) for linker in instrument_linker.linkers)
        ) for row in table.rows
    )

    return LinkedTable(
        studytable_name=instrument_linker.studytable_name,
        data=LinkedTableData(
            column_ids=dst_column_ids,
            rows=rows,
        )
    )
