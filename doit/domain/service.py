import typing as t
from itertools import starmap
from .value import *

def is_integer_text_column(values: t.Sequence[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column(column: ColumnImport) -> SourceColumn:
    match column.type:
        case 'safe_bool':
            (column_type, values) = ('bool', column.values)
        case 'safe_text':
            (column_type, values) = ('text', column.values)
        case 'safe_ordinal':
            (column_type, values) = ('ordinal', column.values)
        case 'unsafe_text':
            # TODO: Sanitize
            column_type='text'
            values=[None for _ in column.values]
        case 'unsafe_numeric_text' if is_integer_text_column(column.values):
            # TODO: Sanitize
            column_type='integer'
            values=[None if i is None else int(i) for i in column.values]
        case 'unsafe_numeric_text':
            # TODO: Sanitize
            column_type='real'
            values=[None if i is None else float(i) for i in column.values],
            
    return new_source_column(
        column_id=column.column_id,
        meta=SourceColumnMeta(
            column_id=column.column_id,
            prompt=column.prompt,
            type=column_type,
            sanitizer_meta="TODO"
        ),
        column_type=column_type,
        values=values
    )

def sanitize_table(table: UnsafeSourceTable) -> SourceTable:
    safe_columns = { column_id: sanitize_column(column) for (column_id, column) in table.table.columns.items() }
    column_meta = { column_id: column.meta for (column_id, column) in safe_columns.items() }

    return SourceTable(
        instrument_id=table.instrument_id,
        columns=safe_columns,
        meta=SourceTableMeta(
            instrument_id=table.instrument_id,
            source_info=table.fetch_info,
            columns=column_meta,
        ),
    )

def stub_instrument_item(column_id: ColumnId, column: SourceColumn) -> InstrumentItem:
    return QuestionInstrumentItem(
        type='question',
        remote_id=column_id,
        measure_id=None,
        prompt=column.meta.prompt,
        map={ i: None for i in column.values if i is not None } if column.type=='ordinal' else None,
    )

def stub_instrument(table: SourceTable) -> InstrumentSpec:
    return InstrumentSpec(
        instrument_id=table.instrument_id,
        title=table.meta.source_info.title,
        description="description",
        instructions="instructions",
        items=list(starmap(stub_instrument_item, table.columns.items()))
    )


def flatten_measures(measures: t.Mapping[MeasureId, MeasureSpec]) -> t.Mapping[MeasureItemId, MeasureItem]:
    def trav(curr_path: MeasureItemId, cursor: t.Mapping[MeasureNodeTag, MeasureNode]) -> t.List[t.Tuple[MeasureItemId, MeasureItem]]:
        return sum([
            trav(curr_path / id, item.items) if item.type == 'group' else [(curr_path / id, item)]
            for (id, item) in cursor.items()
        ], [])
    everything = sum([
        trav(MeasureItemId(id), measure.items) for (id, measure) in measures.items()
    ], [])
    return dict(everything)   

def flatten_instrument_items(items: t.Sequence[InstrumentNode]) -> t.Sequence[InstrumentItem]:
    def trav(items: t.Sequence[InstrumentNode]) -> t.Sequence[InstrumentItem]:
        return sum([ trav(i.items) if i.type == 'group' else [i] for i in items], [])
    return trav(items)

def form_study_table_id(flat_instrument_items: t.Sequence[InstrumentItem], index_uris: t.FrozenSet[MeasureItemId]) -> StudyTableId:
    return StudyTableId(frozenset([
        i.id for i in flat_instrument_items if i.id in index_uris
    ]))

def extract_codemaps(measures: t.Mapping[MeasureId, MeasureSpec]) -> t.Mapping[CodeMapUri, CodeMap]:
    everything = sum([
        [(CodeMapUri(measure_id) / codemap_tag, codemap) for (codemap_tag, codemap) in measure.codes.items()]
            for (measure_id, measure) in measures.items()
    ], [])
    return dict(everything)

def filter_index_measures(measure_items: t.Mapping[MeasureItemId, MeasureItem]) -> t.FrozenSet[MeasureItemId]:
    def is_measure_item_idx(item: MeasureItem) -> bool:
        return item.type == 'ordinal' and item.is_idx is not None and item.is_idx
    return frozenset({ uri for (uri, measure_item) in measure_items.items() if is_measure_item_idx(measure_item)})

def tables_inv(instruments: t.Mapping[InstrumentId, InstrumentSpec], index_uris: t.FrozenSet[MeasureItemId]) -> t.Mapping[MeasureItemId, StudyTableId]:
    result: t.Mapping[MeasureItemId, StudyTableId]= {}
    for instrument in instruments.values():
        flat_items = flatten_instrument_items(instrument.items)
        table_id = form_study_table_id(flat_items, index_uris)
        if table_id:
            for item in flat_items:
                if item.id is not None:
                    existing_value = result.get(item.id)
                    if existing_value is not None:
                        if existing_value != table_id:
                            raise ValueError("Error: expected {} to be indexed by {}; instead found {}".format(instrument.instrument_id, ", ".join(existing_value), ", ".join(table_id)))
                    else:
                        result[item.id] = table_id

    return result


def link_study(instruments: t.Mapping[InstrumentId, InstrumentSpec], measures: t.Mapping[MeasureId, MeasureSpec]) -> StudySpec:
#    measure_items = flatten_measures(measures)
#    index_uris = filter_index_measures(measure_items)
    return StudySpec(
        title="Study title",
        description=None,
        instruments=instruments,
        measures=measures,
#        measure_items=measure_items,
#        codemaps=extract_codemaps(measures),
#        tables=invert_map(tables_inv(instruments, index_uris))
    )