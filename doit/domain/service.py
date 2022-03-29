import typing as t
from itertools import starmap, groupby

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

def flatten_measures(measures: t.Mapping[MeasureId, MeasureSpec]) -> t.Dict[MeasureItemId, MeasureItem]:
    def trav(curr_path: MeasureItemId, cursor: t.Mapping[MeasureNodeTag, MeasureNode]) -> t.Dict[MeasureItemId, MeasureItem]:
        return merge_mappings([
            trav(curr_path / id, item.items) if item.type == 'group' else {(curr_path / id): item}
            for (id, item) in cursor.items()
        ])
    return merge_mappings([
        trav(MeasureItemId(id), measure.items) for (id, measure) in measures.items()
    ])

def flatten_instrument_items(items: t.Sequence[InstrumentNode]) -> t.Sequence[InstrumentItem]:
    return sum([ flatten_instrument_items(i.items) if i.type == 'group' else [i] for i in items], [])

def instruments_to_table_specs(instruments: t.Sequence[InstrumentSpec]) -> t.Set[TableSpec]:
    def conv(instrument_spec: InstrumentSpec) -> TableSpec:
        all_columns = frozenset((i.id for i in flatten_instrument_items(instrument_spec.items) if i.id is not None))
        indices = frozenset((tag.removeprefix('indices.') for tag in all_columns if tag.startswith('indices.')))
        columns = frozenset((tag for tag in all_columns if not tag.startswith('index.')))
        print(all_columns)
        print(columns)
        return TableSpec(
            indices=indices,
            columns=columns,
        )
    return { 
        TableSpec(
            indices=k,
            columns=frozenset(sum(map(lambda i: list(i.columns), v), []))
        )
        for k, v in groupby(
            map(conv, instruments),
            lambda i: i.indices,
        ) 
    }