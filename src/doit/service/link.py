from __future__ import annotations
import typing as t


from ..common.table import (
    Some,
    lookup_fn,
    lookup_fn_seq,
    cast_fn,
)

from ..linker.model import (
    Linker,
    InstrumentLinker,
)

from ..study.view import (
    InstrumentLinkerSpec,
    LinkerSpec,
    OrdinalDstLink,
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
)

from ..study.model import (
    LinkedColumnInfo,
    LinkedTableRowView,
    LinkedTableData,
    LinkedTable,
    LinkedColumnId,
)

def link_value(
    column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo],
    spec: LinkerSpec,
    row: SanitizedTableRowView,
):
    src, dst = spec.src, spec.dst
    match src:
        case QuestionSrcLink():
            src_name = SanitizedColumnId(src.source_column_name)
            
            column_info = column_lookup.get(src_name)
            if not column_info:
                raise Exception("Error: cannot find column in instrument source table ({})".format(src.source_column_name))

            tv = row.get(src_name)

            source_value_map = src.source_value_map

            match column_info.value_type:
                case 'text':
                    source_value = (
                        tv.assert_type(str)
                          .map(lambda v: source_value_map.get(v, v)) # pyright bug: this should be known via union type
                    )
                case 'ordinal':
                    assert isinstance(column_info, SanitizedOrdinalColumnInfo)
                    source_value = (
                        tv.assert_type(int)
                          .bind(lookup_fn(column_info.codes))
                          .map(lambda v: source_value_map.get(v, v))
                    ) 
                case 'multiselect':
                    assert isinstance(column_info, SanitizedOrdinalColumnInfo) # pyright bug: this should be known via union type
                    source_value = (
                        tv.assert_type_seq(int)
                          .bind(lookup_fn_seq(column_info.codes))
                          .map(lambda v: tuple(source_value_map.get(i, i) for i in v))
                    )

        case ConstantSrcLink():
            source_value = Some(src.constant_value)

    source_value_single = source_value.assert_type(str)
    source_value_seq = source_value.assert_type_seq(str)

    match dst.type:
        case 'ordinal' | 'categorical' | 'index':
            assert isinstance(dst, OrdinalDstLink) # pyright bug: this should be known by union type
            result = source_value_single.bind(lookup_fn(dst.value_from_tag))
        case 'multiselect':
            assert isinstance(dst, OrdinalDstLink) # pyright bug: this should be known by union type
            result = source_value_seq.bind(lookup_fn_seq(dst.value_from_tag))
        case 'text':
            result = source_value_single.bind(cast_fn(str))
        case 'real':
            result = source_value_single.bind(cast_fn(float))
        case 'integer':
            result = source_value_single.bind(cast_fn(int))

    return (LinkedColumnId(dst.linked_name), result)

def linker_from_spec(column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo], spec: LinkerSpec):
    return Linker(
        dst_col_id=LinkedColumnId(spec.dst.linked_name),
        dst_col_type=spec.dst.type,
        link_fn=(lambda row: link_value(column_lookup, spec, row)),
    )

def link_tableinfo(tableinfo: SanitizedTableInfo, instrumentlinker_spec: InstrumentLinkerSpec) -> InstrumentLinker:
    column_lookup = { c.id: c for c in tableinfo.columns }
    return InstrumentLinker(
        studytable_name=instrumentlinker_spec.studytable_name,
        instrument_name=instrumentlinker_spec.instrument_name,
        linkers=tuple(
            linker_from_spec(column_lookup, spec)
                for spec in instrumentlinker_spec.linker_specs
        ),
    )

def link_table(table: SanitizedTableData, instrument_linker: InstrumentLinker) -> LinkedTable:
    dst_column_info = tuple(
        LinkedColumnInfo(
            id=row_linker.dst_col_id,
            value_type=row_linker.dst_col_type,
        ) for row_linker in instrument_linker.linkers
    )

    rows = tuple(
        LinkedTableRowView(
            linker.link_fn(row) for linker in instrument_linker.linkers
        ) for row in table.rows
    )

    return LinkedTable(
        studytable_name=instrument_linker.studytable_name,
        columns=dst_column_info,
        data=LinkedTableData(
            column_ids=tuple(c.id for c in dst_column_info),
            rows=rows,
        )
    )
