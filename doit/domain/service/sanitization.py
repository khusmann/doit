import typing as t
from itertools import count
from ..value import *
from ..model import *

default_source_id_gen = count(0)

def is_integer_text_column(values: t.Sequence[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column_data(parent_table_id: SourceTableEntryId, column: ColumnImport) -> SourceColumn:
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
            
    return SourceColumn(
        entry=SourceColumnEntry(
            id=next(default_source_id_gen),
            parent_table_id=parent_table_id,
            name=column.source_column_name,
            content=SourceColumnInfo(
                name=column.source_column_name,
                prompt=column.prompt,
                type=column_type,
            ),
        ),
        values=values,
    )

def sanitize_table(table: UnsafeTable) -> SourceTable: # sanitizers: t.Mapping[SourceColumnName, ColumnSanitizer]

    table_entry_id = SourceTableEntryId(next(default_source_id_gen))

    columns = { column.source_column_name: sanitize_column_data(table_entry_id, column) for column in table.columns }

    table_entry = SourceTableEntry(
        id=table_entry_id,
        name=table.instrument_name,
        content=table.source_table_info,
        columns={ name: column.entry for name, column in columns.items() },
    )

    return SourceTable(
        name=table.instrument_name,
        entry=table_entry,
        columns=columns,
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
        title=table.source_table_info.remote_title,
        description="description",
        instructions="instructions",
        items=(stub_instrument_item(column) for column in table.columns)
    )

