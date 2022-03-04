import typing as t
from .value import *

def is_integer_text_column(values: t.List[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column(column: UnsafeColumnData) -> SafeColumnData:
    match column:
        case UnsafeNumericTextColumnData() if is_integer_text_column(column.values):
            # TODO: Sanitize
            return SafeIntegerColumnData(
                column_id=column.column_id,
                prompt=column.prompt,
                type="integer",
                status="safe",
                values=[None if i is None else int(i) for i in column.values],
            )
        case UnsafeNumericTextColumnData():
            # TODO: Sanitize
            return SafeRealColumnData(
                column_id=column.column_id,
                prompt=column.prompt,
                type="real",
                status="safe",
                values=[None if i is None else float(i) for i in column.values],
            )
        case UnsafeTextColumnData():
            # TODO: Sanitize
            return SafeTextColumnData(
                column_id=column.column_id,
                prompt=column.prompt,
                type="text",
                status="safe",
                values=[None for _ in column.values],
            )
        

def sanitize_table(table: UnsafeTable) -> SafeTable:
    return SafeTable(
        title=table.title,
        columns={
            column_id: data if data.status == "safe" else sanitize_column(data)
                for (column_id, data) in table.columns.items()
        }
    )