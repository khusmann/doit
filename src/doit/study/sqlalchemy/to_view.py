import typing as t
from pydantic import parse_obj_as

from .sqlmodel import (
    CodemapSql,
    ColumnEntrySql,
    ColumnEntryType,
    InstrumentEntrySql,
    InstrumentNodeSql,
    InstrumentNodeType,
    MeasureEntrySql,
    StudyTableSql,
)

from ..view import (
    CodemapRaw,
    ConstantInstrumentNodeView,
    DstLink,
    CodedDstLink,
    InstrumentListingItemView,
    InstrumentListingView,
    MeasureListingItemView,
    MeasureListingView,
    SimpleDstLink,
    GroupInstrumentNodeView,
    GroupMeasureNodeView,
    InstrumentNodeView,
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    CodedColumnView,
    CodedMeasureNodeView,
    QuestionInstrumentNodeView,
    SimpleColumnView,
    SimpleMeasureNodeView,
    CodemapView,
    IndexColumnView,
    SrcLink,
    ConstantSrcLink,
    QuestionSrcLink,
    StudyTableView,
    LinkerSpec,
    InstrumentLinkerSpec,
)

SQL_MISSING_TEXT = "Error: missing value in DB"

def to_measureview(entry: MeasureEntrySql):
    return MeasureView(
        name=entry.name,
        title=entry.title or SQL_MISSING_TEXT,
        description=entry.description,
        items=tuple(
            to_measurenodeview(n) for n in entry.items
                if n.parent_column_id is None
        ),
    )

def to_codemapview(entry: CodemapSql) -> CodemapView:
    codemap = parse_obj_as(CodemapRaw, entry.values)

    return CodemapView(
        tag_from_value={ i['value']: i['tag'] for i in codemap },
        label_from_value={ i['value']: i['text'] for i in codemap },
        label_from_tag={ i['tag']: i['text'] for i in codemap },
        value_from_tag={ i['tag']: i['value'] for i in codemap },
        values=codemap,
    )

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL | ColumnEntryType.MULTISELECT:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return CodedMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                value_type=entry.type.value,
                codes=to_codemapview(entry.codemap)
            )
        case ColumnEntryType.TEXT | ColumnEntryType.REAL | ColumnEntryType.INTEGER:
            return SimpleMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                value_type=entry.type.value,
            )
        case ColumnEntryType.GROUP:
            return GroupMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                items=tuple(
                    to_measurenodeview(i) for i in entry.items
                ),
            )
        case ColumnEntryType.INDEX:
            raise Exception("Error: Found index column {} in a measure definition".format(entry.name))

def to_instrumentnodeview(entry: InstrumentNodeSql) -> InstrumentNodeView:
    match entry.type:
        case InstrumentNodeType.QUESTION:
            return QuestionInstrumentNodeView(
                prompt=entry.prompt or SQL_MISSING_TEXT,
                source_column_name=entry.source_column_name,
                map=parse_obj_as(t.Mapping[str, t.Optional[str]], entry.source_value_map) if entry.source_value_map else {},
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None,
            )
        case InstrumentNodeType.CONSTANT:
            if not entry.constant_value:
                raise Exception("Error: constant instrument item node missing value")
            return ConstantInstrumentNodeView(
                constant_value=entry.constant_value,
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None,
            )
        case InstrumentNodeType.GROUP:
            return GroupInstrumentNodeView(
                title=entry.title,
                prompt=entry.prompt,
                items=tuple(
                    to_instrumentnodeview(i) for i in entry.items
                )
            )

def to_instrumentview(entry: InstrumentEntrySql):
    return InstrumentView(
        name=entry.name,
        title=entry.title or SQL_MISSING_TEXT,
        description=entry.description,
        instructions=entry.instructions,
        nodes=tuple(
            to_instrumentnodeview(i) for i in entry.items
                if i.parent_node_id is None
        ),
    )

def to_instrumentlistingview(entries: t.Sequence[InstrumentEntrySql]):
    return InstrumentListingView(
        items=tuple(
            InstrumentListingItemView(
                name=i.name,
                title=i.title or SQL_MISSING_TEXT,
            ) for i in entries
        )
    )

def to_measurelistingview(entries: t.Sequence[MeasureEntrySql]):
    return MeasureListingView(
        items=tuple(
            MeasureListingItemView(
                name=i.name,
                title=i.title or SQL_MISSING_TEXT,
            ) for i in entries
        )
    )


def to_columnview(entry: ColumnEntrySql) -> ColumnView:
    studytable_name = entry.studytables[0].name if len(entry.studytables) == 1 else None
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL | ColumnEntryType.MULTISELECT:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return CodedColumnView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                value_type=entry.type.value,
                studytable_name=studytable_name,
                codes=to_codemapview(entry.codemap)
            )

        case ColumnEntryType.INDEX:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return IndexColumnView(
                name=entry.name,
                title=entry.title or SQL_MISSING_TEXT,
                description=entry.description,
                value_type=entry.type.value,
                codes=to_codemapview(entry.codemap)
            )

        case ColumnEntryType.REAL | ColumnEntryType.TEXT | ColumnEntryType.INTEGER:
            return SimpleColumnView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                studytable_name=studytable_name,
                type=entry.type.value,
            )

        case ColumnEntryType.GROUP:
            raise Exception("Error: Group columns cannot be returned as ColumnViews")


def to_studytableview(entry: StudyTableSql) -> StudyTableView:
    return StudyTableView(
        name=entry.name,
        columns=tuple(to_columnview(i) for i in entry.columns),
    )

def to_srcconnectionview(entry: InstrumentNodeSql) -> SrcLink:
    match entry.type:
        case InstrumentNodeType.QUESTION:
            if not entry.source_column_name:
                raise Exception("Error: cannot make question instrument connection without source column name")
            return QuestionSrcLink(
                source_column_name=entry.source_column_name,
                source_value_map=parse_obj_as(t.Mapping[str, str], entry.source_value_map) if entry.source_value_map else {},
            )
        case InstrumentNodeType.CONSTANT:
            if not entry.constant_value:
                raise Exception("Error: cannot make constant instrument connection without a value") 
            return ConstantSrcLink(
                constant_value=entry.constant_value,
            )
        case InstrumentNodeType.GROUP:
            raise Exception("Error: cannot link from type {}".format(entry.type))

def to_dstconnectionview(entry: ColumnEntrySql) -> DstLink:
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL | ColumnEntryType.INDEX | ColumnEntryType.MULTISELECT:
            if not entry.codemap:
                raise Exception("Error: ordinal column {} missing codemap".format(entry.name))

            codemap = parse_obj_as(CodemapRaw, entry.codemap.values)

            return CodedDstLink(
                linked_name=entry.name,
                value_from_tag={ i['tag']: i['value'] for i in codemap },
                value_type=entry.type.value,
            )
        case ColumnEntryType.REAL | ColumnEntryType.INTEGER | ColumnEntryType.TEXT:
            return SimpleDstLink(
                linked_name=entry.name,
                value_type=entry.type.value,
            )
        case ColumnEntryType.GROUP:
            raise Exception("Error: Group column types cannot be linked!")

def to_instrumentlinkerspec(entry: InstrumentEntrySql):
    studytable = entry.studytable
    if studytable is None:
        raise Exception("Error: Instrument entry has no StudyTable")
    return InstrumentLinkerSpec(
        studytable_name=studytable.name,
        instrument_name=entry.name,
        linker_specs=tuple(
            LinkerSpec(
                src=to_srcconnectionview(i),
                dst=to_dstconnectionview(i.column_entry),
            ) for i in entry.items
                if i.column_entry is not None and
                    (i.type == InstrumentNodeType.CONSTANT or i.source_column_name is not None)
        ),
    )