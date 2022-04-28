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
    FromFn,
    ToFn,
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

def from_question(column_info: SanitizedColumnInfo, src: QuestionSrcLink ) -> FromFn:
    src_name = SanitizedColumnId(src.source_column_name)
    match column_info:
        case SanitizedColumnInfo():
            # TODO: ordinal? multiselect?
            def from_text(row: SanitizedTableRowView):
                return row.get(src_name)
            return from_text

def from_src(
    column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo],
    src: SrcLink,
) -> FromFn:
    match src:
        case QuestionSrcLink():
            column_info = column_lookup.get(SanitizedColumnId(src.source_column_name))
            if not column_info:
                raise Exception("Error: cannot find column in instrument source table ({})".format(src.source_column_name))
            return from_question(column_info, src)
        case ConstantSrcLink():
            def from_constant(_: SanitizedTableRowView):
                return Some(src.constant_value)
            return from_constant

def to_ordinal(dst: OrdinalDstLink) -> ToFn:
    def inner(tablevalue: TableValue):
        match tablevalue:
            case Some(value=value) if isinstance(value, str):
                result = Some(dst.codes.tags[value]) # type: ignore TODO: this won't work; need to actually map tag->value
            case Some(value=value):
                result = ErrorValue(IncorrectTypeError(value))
            case Omitted() | Redacted() | ErrorValue():
                result = tablevalue
        return LinkedTableRowView({
            LinkedColumnId(dst.linked_name): result
        })
    return inner

def to_simple(dst: SimpleDstLink) -> ToFn:
    def inner(tablevalue: TableValue):
        return LinkedTableRowView({
            LinkedColumnId(dst.linked_name): tablevalue
        })
    return inner

def to_dst(dst: DstLink) -> ToFn:
    match dst:
        case OrdinalDstLink():
            return to_ordinal(dst)
        case SimpleDstLink():
            return to_simple(dst)

def linker_from_spec(column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo], spec: LinkerSpec):
    match spec.dst:
        case OrdinalDstLink() | SimpleDstLink():
            return Linker(
                dst_col_ids=(LinkedColumnId(spec.dst.linked_name),),
                from_src=from_src(column_lookup, spec.src),
                to_dst=to_dst(spec.dst),
            )

def link_tableinfo(tableinfo: SanitizedTableInfo, instrumentlinker_spec: InstrumentLinkerSpec) -> InstrumentLinker:
    column_lookup = { c.id: c for c in tableinfo.columns }
    return InstrumentLinker(
        studytable_name=instrumentlinker_spec.studytable_name,
        linkers=tuple(linker_from_spec(column_lookup, rowlinker_spec) for rowlinker_spec in instrumentlinker_spec.linker_specs),
    )

def link_tabledata(table: SanitizedTableData, instrument_linker: InstrumentLinker) -> LinkedTableData:
    dst_column_ids = tuple(
        i
            for row_linker in instrument_linker.linkers
                for i in row_linker.dst_col_ids
    )

    rows = tuple(
        LinkedTableRowView.combine_views(
            *(row_linker.to_dst(row_linker.from_src(row)) for row_linker in instrument_linker.linkers)
        ) for row in table.rows
    )

    return LinkedTableData(
        column_ids=dst_column_ids,
        rows=rows,
    )
