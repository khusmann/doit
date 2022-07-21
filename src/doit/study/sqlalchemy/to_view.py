import typing as t
from pydantic import parse_obj_as

from doit.study.spec import CompareExcludeFilter, MatchExcludeFilter

from .sqlmodel import (
    CodemapSql,
    ColumnEntrySql,
    ColumnEntryType,
    CompositeDependencySql,
    InstrumentEntrySql,
    InstrumentNodeSql,
    InstrumentNodeType,
    MeasureEntrySql,
)

from ..view import (
    AggregateItemSpec,
    AggregateSpec,
    CodemapRaw,
    ColumnRawView,
    CompareExcludeFilterSpec,
    CompositeColumnView,
    CompositeMeasureNodeView,
    ConstantInstrumentNodeView,
    DstLink,
    CodedDstLink,
    InstrumentListingItemView,
    InstrumentListingView,
    MatchExcludeFilterSpec,
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

def dep_to_str(dep: CompositeDependencySql) -> str:
    if dep.reverse_coded:
        return "rev({})".format(dep.dependency.name)
    return dep.dependency.name

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
                prompt=entry.prompt,
                items=tuple(
                    to_measurenodeview(i) for i in entry.items
                ),
            )
        case ColumnEntryType.COMPOSITE:
            assert entry.composite_type
            return CompositeMeasureNodeView(
                name=entry.name,
                title=entry.title or SQL_MISSING_TEXT,
                composite_type=entry.composite_type.value,
                dependencies=tuple(dep_to_str(d) for d in entry.dependencies)
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
        items=tuple(sorted((
            InstrumentListingItemView(
                name=i.name,
                title=i.title or SQL_MISSING_TEXT,
                description=i.description or SQL_MISSING_TEXT,
                indices=tuple(idx.column_entry.name for idx in i.items if idx.column_entry and idx.column_entry.type == ColumnEntryType.INDEX)
            ) for i in entries
        ), key=lambda x: x.title))
    )

def to_measurelistingview(entries: t.Sequence[MeasureEntrySql]):
    return MeasureListingView(
        items=tuple(sorted((
            MeasureListingItemView(
                name=i.name,
                title=i.title or SQL_MISSING_TEXT,
                description=i.description or SQL_MISSING_TEXT,
                indices=tuple(i.indices),
            ) for i in entries
        ), key=lambda x: x.title))
    )


def to_columnview(entry: ColumnEntrySql) -> ColumnView:
    match entry.type:
        case ColumnEntryType.ORDINAL | ColumnEntryType.CATEGORICAL | ColumnEntryType.MULTISELECT:
            if not entry.codemap:
                raise Exception("Error: missing codemap")

            return CodedColumnView(
                name=entry.name,
                prompt=entry.prompt or SQL_MISSING_TEXT,
                value_type=entry.type.value,
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
                value_type=entry.type.value,
            )

        case ColumnEntryType.COMPOSITE:
            assert entry.composite_type
            return CompositeColumnView(
                name=entry.name,
                title=entry.title or SQL_MISSING_TEXT,
                composite_type=entry.composite_type.value,
            )

        case ColumnEntryType.GROUP:
            raise Exception("Error: Group columns cannot be returned as ColumnViews")


def to_srcconnectionview(entry: InstrumentNodeSql) -> SrcLink:
    match entry.type:
        case InstrumentNodeType.QUESTION:
            if not entry.source_column_name:
                raise Exception("Error: cannot make question instrument connection without source column name")
            return QuestionSrcLink(
                source_column_name=entry.source_column_name,
                source_value_map=parse_obj_as(t.Mapping[str, t.Optional[str]], entry.source_value_map) if entry.source_value_map else {},
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
        case ColumnEntryType.COMPOSITE:
            raise Exception("Error: Composite column types cannot be linked!")
        case ColumnEntryType.GROUP:
            raise Exception("Error: Group column types cannot be linked!")

def to_excludefilter(raw: t.Any):
    from ..spec import ExcludeFilter
    spec_filter = parse_obj_as(ExcludeFilter, raw)

    match spec_filter:
        case MatchExcludeFilter():
            return MatchExcludeFilterSpec(
                type=spec_filter.type,
                values=spec_filter.values,
            )
        case CompareExcludeFilter():
            return CompareExcludeFilterSpec(
                type=spec_filter.type,
                column=spec_filter.column,
                value=spec_filter.value,
            )

def to_aggregateitemspec(dep: CompositeDependencySql):
    if dep.reverse_coded:
        if not dep.dependency.codemap:
            raise Exception("Expected codemap for: {}".format(dep.dependency.name))
        forward = sorted(i['value'] for i in to_codemapview(dep.dependency.codemap).values)
        reverse = reversed(forward)
        return AggregateItemSpec(
            name=dep.dependency.name,
            value_map={f: r for f, r in zip(forward, reverse)},
        )
    return AggregateItemSpec(
        name=dep.dependency.name,
        value_map={},
    )

def to_instrumentlinkerspec(entry: InstrumentEntrySql):
    linkable_columns = tuple(
        i for i in entry.items 
            if (i.type == InstrumentNodeType.CONSTANT or i.source_column_name is not None)
    )

    connected_measures = { i.column_entry.parent_measure for i in entry.items if i.column_entry is not None and i.column_entry.parent_measure is not None}

    composites = tuple(
        c for m in connected_measures
            for c in m.items
                if c.type == ColumnEntryType.COMPOSITE
    )

    return InstrumentLinkerSpec(
        instrument_name=entry.name,
        exclude_filters=tuple(to_excludefilter(ef) for ef in entry.exclude_filters) if entry.exclude_filters else (),
        linker_specs=tuple(
            LinkerSpec(
                src=to_srcconnectionview(i),
                dst=to_dstconnectionview(i.column_entry),
            ) for i in linkable_columns if i.column_entry is not None
        ),
        aggregate_specs=tuple(
           AggregateSpec(
               linked_name=i.name,
               composite_type=i.composite_type.value,
               items=tuple(to_aggregateitemspec(j) for j in i.dependencies),
           ) for i in composites if i.composite_type # Hrumph, don't like this if... need better typing...
        )
    )

def to_columnrawview(entry: ColumnEntrySql):
    if not entry.parent_measure:
        raise Exception("Error, no parent measure for {}".format(entry))

    return ColumnRawView(
        name=entry.name,
        table_name=entry.parent_measure.name,
        indices=tuple(entry.parent_measure.indices),
    )