from __future__ import annotations
import typing as t
from functools import partial

from ..common.table import (
    Some,
    TableValue,
    lookup_fn,
    lookup_fn_seq,
    cast_fn,
)

from ..study.spec import (
    InstrumentSpec,
    QuestionInstrumentItemSpec,
)

from ..linker.model import (
    Linker,
    InstrumentLinker,
    LinkFn,
)

from ..study.view import (
    CodedDstLink,
    DstLink,
    InstrumentLinkerSpec,
    LinkerSpec,
    ConstantSrcLink,
    QuestionSrcLink,
    SimpleDstLink,
    SrcLink,
)

from ..sanitizedtable.model import (
    SanitizedCodedColumnInfo,
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTable,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRowView,
    SanitizedSimpleColumnInfo,
)

from ..study.model import (
    LinkedColumnInfo,
    LinkedTableRowView,
    LinkedTableData,
    LinkedTable,
    LinkedColumnId,
)

def from_source_fn(
    column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo],
    src: SrcLink,
) -> t.Callable[[SanitizedTableRowView], TableValue[t.Any]]:
    match src:
        case QuestionSrcLink():
            column_info = column_lookup.get(SanitizedColumnId(src.source_column_name))
            
            if not column_info:
                raise Exception("Error: cannot find column in instrument source table ({})".format(src.source_column_name))

            return partial(from_source_question, column_info, src.source_value_map)

        case ConstantSrcLink():
            return lambda _: Some(src.constant_value)

def from_source_question(
    column_info: SanitizedColumnInfo,
    source_value_map: t.Mapping[str, str],
    row: SanitizedTableRowView
) -> TableValue[t.Any]:
    tv = row.get(column_info.id)

    match column_info:
        case SanitizedCodedColumnInfo():
            match column_info.value_type:
                case 'multiselect':
                    return (
                        tv.assert_type_seq(int)
                          .bind(lookup_fn_seq(column_info.codes))
                          .map(lambda v: tuple(source_value_map.get(i, i) for i in v))
                    )
                case 'ordinal':
                    return (
                        tv.assert_type(int)
                          .bind(lookup_fn(column_info.codes))
                          .map(lambda v: source_value_map.get(v, v))
                    )

        case SanitizedSimpleColumnInfo():
            match column_info.value_type:
                case 'text':
                    return (
                        tv.assert_type(str)
                          .map(lambda v: source_value_map.get(v, v))
                    )

def to_dst(
    source_value: TableValue[t.Any],
    dst: DstLink,
):
    single = source_value.assert_type(str)
    seq = source_value.assert_type_seq(str)

    match dst:
        case CodedDstLink():
            match dst.value_type:
                case 'multiselect':
                    result = seq.bind(lookup_fn_seq(dst.value_from_tag))
                case 'ordinal' | 'categorical' | 'index':
                    result = single.bind(lookup_fn(dst.value_from_tag))

        case SimpleDstLink():
            match dst.value_type:
                case 'text':
                    result = single.bind(cast_fn(str))
                case 'real':
                    result = single.bind(cast_fn(float))
                case 'integer':
                    result = single.bind(cast_fn(int))

    return (LinkedColumnId(dst.linked_name), result)

def link_tableinfo(tableinfo: SanitizedTableInfo, instrumentlinker_spec: InstrumentLinkerSpec) -> InstrumentLinker:
    column_lookup = { c.id: c for c in tableinfo.columns }

    def build_link_fn(spec: LinkerSpec) -> LinkFn:
        src_fn = from_source_fn(column_lookup, spec.src)
        return lambda row: to_dst(src_fn(row), spec.dst)

    return InstrumentLinker(
        studytable_name=instrumentlinker_spec.studytable_name,
        instrument_name=instrumentlinker_spec.instrument_name,
        linkers=tuple(
            Linker(
                dst_col_id=LinkedColumnId(spec.dst.linked_name),
                dst_col_type=spec.dst.value_type,
                link_fn=build_link_fn(spec),
            ) for spec in instrumentlinker_spec.linker_specs
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
        instrument_name=instrument_linker.instrument_name,
        columns=dst_column_info,
        data=LinkedTableData(
            column_ids=tuple(c.id for c in dst_column_info),
            rows=rows,
        )
    )

def stub_columnspec(column: SanitizedColumnInfo):
    match column:
        case SanitizedCodedColumnInfo():
            map = { key: None for key in column.codes.values()}
        case SanitizedSimpleColumnInfo():
            map = None
    
    return QuestionInstrumentItemSpec(
        prompt=column.prompt,
        type='question',
        remote_id=column.id.name,
        id=None,
        map=map,
    )

def stub_instrumentspec(table: SanitizedTable) -> InstrumentSpec:
    return InstrumentSpec(
        title=table.info.title,
        description="enter description here",
        instructions="enter instructions here",
        items=tuple(
            stub_columnspec(i) for i in table.info.columns
        )
    )