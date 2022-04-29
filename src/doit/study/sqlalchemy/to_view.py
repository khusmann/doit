from .sqlmodel import (
    CodemapSql,
    ColumnEntrySql,
    ColumnEntryType,
    InstrumentEntrySql,
    InstrumentNodeSql,
    MeasureEntrySql,
    StudyTableSql,
)

from ..view import (
    CodemapRaw,
    ConstantInstrumentNodeView,
    DstLink,
    OrdinalDstLink,
    SimpleDstLink,
    GroupInstrumentNodeView,
    GroupMeasureNodeView,
    InstrumentNodeView,
    InstrumentView,
    MeasureNodeView,
    ColumnView,
    MeasureView,
    OrdinalColumnView,
    OrdinalMeasureNodeView,
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


def to_measureview(entry: MeasureEntrySql):
    return MeasureView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        items=tuple(
            to_measurenodeview(n) for n in entry.items
                if n.parent_column_id is None
        ),
    )

def to_codemapview(entry: CodemapSql) -> CodemapView:
    codemap = CodemapRaw.parse_obj({ 'values': entry.values })

    return CodemapView(
        tags={i.value: i.tag for i in codemap.values},
        labels={i.value: i.text for i in codemap.values},
    )

def to_measurenodeview(entry: ColumnEntrySql) -> MeasureNodeView:
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return OrdinalMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry.type.value,
                codes=to_codemapview(entry.codemap)
            )
        case ColumnEntryType.TEXT | ColumnEntryType.REAL | ColumnEntryType.INTEGER:
            return SimpleMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry.type.value,
            )
        case ColumnEntryType.GROUP:
            return GroupMeasureNodeView(
                name=entry.name,
                prompt=entry.prompt,
                items=tuple(
                    to_measurenodeview(i) for i in entry.items
                ),
            )
        case ColumnEntryType.INDEX:
            raise Exception("Error: Found index column {} in a measure definition".format(entry.name))

def to_instrumentnodeview(entry: InstrumentNodeSql) -> InstrumentNodeView:
    match entry.type:
        case 'question':
            return QuestionInstrumentNodeView(
                prompt=entry.prompt,
                source_column_name=entry.source_column_name,
                map={},
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None,
            )
        case 'constant':
            return ConstantInstrumentNodeView(
                constant_value=entry.constant_value,
                column_info=to_columnview(entry.column_entry) if entry.column_entry else None,
            )
        case 'group':
            return GroupInstrumentNodeView(
                title=entry.title,
                prompt=entry.prompt,
                items=tuple(
                    to_instrumentnodeview(i) for i in entry.items
                )
            )
        case _:
            raise Exception("Error: Unknown instrument node type {}".format(entry.type))

def to_instrumentview(entry: InstrumentEntrySql):
    return InstrumentView(
        name=entry.name,
        title=entry.title,
        description=entry.description,
        instructions=entry.instructions,
        nodes=tuple(
            to_instrumentnodeview(i) for i in entry.items
                if i.parent_node_id is None
        ),
    )

def to_columnview(entry: ColumnEntrySql) -> ColumnView:
    studytable_name = entry.studytables[0].name if len(entry.studytables) == 1 else None
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return OrdinalColumnView(
                name=entry.name,
                prompt=entry.prompt,
                type=entry.type.value,
                studytable_name=studytable_name,
                codes=to_codemapview(entry.codemap)
            )

        case ColumnEntryType.INDEX:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return IndexColumnView(
                name=entry.name,
                title=entry.title,
                codes=to_codemapview(entry.codemap)
            )

        case ColumnEntryType.REAL | ColumnEntryType.TEXT | ColumnEntryType.INTEGER:
            return SimpleColumnView(
                name=entry.name,
                prompt=entry.prompt,
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
        case "question":
            return QuestionSrcLink(
                source_column_name=entry.source_column_name,
                source_value_map=entry.source_value_map,
            )
        case "constant":
            return ConstantSrcLink(
                constant_value=entry.constant_value,
            )
        case _:
            raise Exception("Error: cannot link from type {}".format(entry.type))

def to_dstconnectionview(entry: ColumnEntrySql) -> DstLink:
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL | ColumnEntryType.INDEX:
            if not entry.codemap:
                raise Exception("Error: ordinal column {} missing codemap".format(entry.name))

            codemap = CodemapRaw.parse_obj({ 'values': entry.codemap.values })

            return OrdinalDstLink(
                linked_name=entry.name,
                value_from_tag={ i.tag: i.value for i in codemap.values },
                type=entry.type.value,
            )
        case ColumnEntryType.REAL | ColumnEntryType.INTEGER | ColumnEntryType.TEXT:
            return SimpleDstLink(
                linked_name=entry.name,
                type=entry.type.value,
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
                if i.source_column_name is not None and i.column_entry is not None
        ),
    )