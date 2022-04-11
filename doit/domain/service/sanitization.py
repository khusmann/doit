import typing as t
from itertools import count
from ..value import *
from ..model import *

default_source_id_gen = count(0)

def is_integer_text_column(values: t.Sequence[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column_data(column: ColumnImport) -> SourceColumnData:
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
            
    return SourceColumnData(
        name=column.source_column_name,
        type=column_type,
        values=values,
    )

def sanitize_table(table: UnsafeTable) -> SourceTable: # sanitizers: t.Mapping[SourceColumnName, ColumnSanitizer]
    data_columns = tuple(sanitize_column_data(column) for column in table.columns)

    table_info_id = next(default_source_id_gen)

    column_info = {
        column.source_column_name: SourceColumnInfo(
            id=next(default_source_id_gen),
            parent_table_id=table_info_id,
            name=column.source_column_name,
            type=data_column.type,
            prompt=column.prompt,
            sanitizer_meta="TODO",
        ) for column, data_column in zip(table.columns, data_columns) 
    }

    table_info = SourceTableInfo(
        id=table_info_id,
        name=table.instrument_name,
        fetch_info=table.fetch_info,
        columns=column_info,
    )

    return SourceTable(
        name=table.instrument_name,
        info=table_info,
        data={ data_column.name: data_column for data_column in data_columns},
    )

def stub_instrument_item(column: ColumnImport) -> InstrumentNodeSpec:
    return QuestionInstrumentItemSpec(
        type='question',
        remote_id=column.source_column_name,
        prompt=column.prompt,
        map={ i: None for i in column.values if i is not None } if column.type=='safe_ordinal' else None,
    )

def stub_instrument_spec(table: UnsafeTable) -> InstrumentSpec:
    return InstrumentSpec(
        title=table.fetch_info.remote_title,
        description="description",
        instructions="instructions",
        items=(stub_instrument_item(column) for column in table.columns)
    )

