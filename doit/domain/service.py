import typing as t
from itertools import starmap
from .value import *

def is_integer_text_column(values: t.Sequence[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column(column: ColumnImport) -> SafeColumn:
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
            
    return new_safe_column(
        column_id=column.column_id,
        meta=SafeColumnMeta(
            column_id=column.column_id,
            prompt=column.prompt,
            type=column_type,
            sanitizer_meta="TODO"
        ),
        column_type=column_type,
        values=values
    )

def sanitize_table(table: UnsafeTable) -> SafeTable:
    assert table.meta.source_info is not None
    safe_columns = { column_id: sanitize_column(column) for (column_id, column) in table.columns.items() }
    column_meta = { column_id: column.meta for (column_id, column) in safe_columns.items() }

    return SafeTable(
        instrument_id=table.instrument_id,
        columns=safe_columns,
        meta=SafeTableMeta(
            instrument_id=table.instrument_id,
            source_info=table.meta.source_info,
            columns=column_meta,
        ),
    )

def stub_instrument_item(column_id: ColumnId, column: SafeColumn) -> InstrumentItem:
    return QuestionInstrumentItem(
        type='question',
        remote_id=column_id,
        measure_id=None,
        prompt=column.meta.prompt,
        map={ i: None for i in column.values if i is not None } if column.type=='ordinal' else None,
    )

def stub_instrument(table: SafeTable) -> Instrument:
    return Instrument(
        instrument_id=table.instrument_id,
        title=table.meta.source_info.title,
        description="description",
        instructions="instructions",
        items=list(starmap(stub_instrument_item, table.columns.items()))
    )

def spec_linkedtables(study: Study) -> t.Sequence[LinkedTableSpec]:
    return [
        LinkedTableSpec(
            instrument_id=instrument_id,
            columns={
                i.measure_id: LinkedColumnSpec(
                    instrument_item=i,
                    measure_item=study.resolve_measure_path(i.measure_id),
                ) for i in instrument.valueitems_flat() if i.measure_id is not None
            },
        ) for (instrument_id, instrument) in study.instruments.items()
    ]