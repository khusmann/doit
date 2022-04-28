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
    Linker
)

from ..study.view import (
    DstLink,
    OrdinalDstLink,
    SimpleDstLink,
    LinkerSpec,
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

def from_question(src: QuestionSrcLink, column_info: SanitizedColumnInfo) -> FromFn:
    src_name = SanitizedColumnId(src.source_column_name)
    match column_info:
        case SanitizedColumnInfo():
            # TODO: ordinal? multiselect?
            def from_text(row: SanitizedTableRowView):
                return row.get(src_name)
            return from_text

def from_src(
    src: SrcLink,
    column_lookup: t.Mapping[SanitizedColumnId, SanitizedColumnInfo]
) -> FromFn:
    match src:
        case QuestionSrcLink():
            column_info = column_lookup.get(SanitizedColumnId(src.source_column_name))
            if not column_info:
                raise Exception("Error: cannot find column in instrument source table ({})".format(src.source_column_name))
            return from_question(src, column_info)
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

def link_tableinfo(info: SanitizedTableInfo, linker_specs: t.Sequence[LinkerSpec]) -> t.Tuple[Linker, ...]:
    column_lookup = { i.id: i for i in info.columns }
    return tuple(
        Linker(
            from_src=from_src(linker_spec.src, column_lookup),
            to_dst=to_dst(linker_spec.dst),
            dst_col_ids=(LinkedColumnId(linker_spec.dst.linked_name),)
        ) for linker_spec in linker_specs
    )

def link_tabledata(table: SanitizedTableData, linkers: t.Sequence[Linker]) -> LinkedTableData:
    dst_column_ids = tuple(
        i
            for linker in linkers
                for i in linker.dst_col_ids
    )

    rows = tuple(
        LinkedTableRowView.combine_views(
            *(linker.to_dst(linker.from_src(row)) for linker in linkers)
        ) for row in table.rows
    )

    return LinkedTableData(
        column_ids=dst_column_ids,
        rows=rows,
    )
