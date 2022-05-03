from __future__ import annotations
import typing as t


from ..common.table import (
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

            tv = row.get(src_name)

            match column_info:
                case SanitizedTextColumnInfo():
                    return tv.lookup_with_default(src.source_value_map, str)
                case SanitizedOrdinalColumnInfo():
                    return (
                        tv.lookup(column_info.codes, int)
                          .lookup_with_default(src.source_value_map, str)
                    ) 

        case ConstantSrcLink():
            return Some(src.constant_value)

def to_dst(dst: DstLink, tv: TableValue):
    match dst:
        case OrdinalDstLink():
            return tv.lookup(dst.value_from_tag, str)
        case SimpleDstLink():
            return tv

def linker_from_spec(column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo], spec: LinkerSpec):
    linked_name = LinkedColumnId(spec.dst.linked_name)
    return Linker(
        dst_col_id=linked_name,
        link_fn=lambda row: ( linked_name, to_dst(spec.dst, from_src(column_lookup, spec.src, row)) ),
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
        row_linker.dst_col_id for row_linker in instrument_linker.linkers
    )

    rows = tuple(
        LinkedTableRowView(
            linker.link_fn(row) for linker in instrument_linker.linkers
        ) for row in table.rows
    )

    return LinkedTable(
        studytable_name=instrument_linker.studytable_name,
        data=LinkedTableData(
            column_ids=dst_column_ids,
            rows=rows,
        )
    )
