from __future__ import annotations
import typing as t


from ..common.table import (
    TableValue,
    Some,
    tv_lookup,
    tv_lookup_with_default,
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

            tv = row.get(src_name)

            match column_info:
                case SanitizedTextColumnInfo():
                    return tv_lookup_with_default(tv, src.source_value_map, str)
                case SanitizedOrdinalColumnInfo() | SanitizedMultiselectColumnInfo():
                    label = tv_lookup(tv, column_info.codes, int)
                    return tv_lookup_with_default(label, src.source_value_map, str)


        case ConstantSrcLink():
            return Some(src.constant_value)

def to_dst(dst: DstLink, tv: TableValue):
    match dst:
        case OrdinalDstLink():
            return tv_lookup(tv, dst.value_from_tag, str)
        case SimpleDstLink():
            return tv

def linker_from_spec(column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo], spec: LinkerSpec):
    linked_name = LinkedColumnId(spec.dst.linked_name)
    return Linker(
        dst_col_ids=(linked_name,),
        link_fn=lambda row: LinkedTableRowView({ linked_name: to_dst(spec.dst, from_src(column_lookup, spec.src, row))}),
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

# TODO: Let link_fn just map from TableRowView -> TableValue

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
