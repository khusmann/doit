import typing as t
from itertools import starmap
from ..value import *
from ..model import *

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
        source_column_name=column.source_column_name,
        meta=SourceColumnInfo(
            source_column_name=column.source_column_name,
            prompt=column.prompt,
            type=column_type,
            sanitizer_meta="TODO"
        ),
        column_type=column_type,
        values=values
    )

def sanitize_table(table: UnsafeTable) -> SourceTable:
    safe_columns = { column_id: sanitize_column(column) for (column_id, column) in table.table.columns.items() }
    column_meta = { column_id: column.meta for (column_id, column) in safe_columns.items() }

    return SourceTable(
        instrument_name=table.instrument_name,
        columns=safe_columns,
        meta=SourceTableInfo(
            instrument_name=table.instrument_name,
            source_info=table.fetch_info,
            columns=column_meta,
        ),
    )

def stub_instrument_item(column_name: SourceColumnName, column: ColumnImport) -> InstrumentNodeSpec:
    return QuestionInstrumentItemSpec(
        type='question',
        remote_id=column_name,
        prompt=column.prompt,
        map={ i: None for i in column.values if i is not None } if column.type=='safe_ordinal' else None,
    )

def stub_instrument_spec(table: UnsafeTable) -> InstrumentSpec:
    return InstrumentSpec(
        title=table.table.title,
        description="description",
        instructions="instructions",
        items=list(starmap(stub_instrument_item, table.table.columns.items()))
    )

