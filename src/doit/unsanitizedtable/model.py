import typing as t

from ..common import (
    TableData,
    TableRowView,
)

class UnsanitizedColumnId(t.NamedTuple):
    unsafe_name: str

class UnsanitizedTextColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    is_safe: bool

class UnsanitizedArrayColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    codes: t.Mapping[str, str]
    is_safe = True

class UnsanitizedOrdinalColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    codes: t.Mapping[str, str]
    is_safe = True

UnsanitizedColumnInfo = UnsanitizedTextColumnInfo | UnsanitizedArrayColumnInfo | UnsanitizedOrdinalColumnInfo

class UnsanitizedTableSchema(t.NamedTuple):
    columns: t.Tuple[UnsanitizedColumnInfo, ...]

UnsanitizedStrTableRowView = TableRowView[UnsanitizedColumnId, str]
UnsanitizedTableRowView = TableRowView[UnsanitizedColumnId, t.Any]

UnsanitizedTableData = TableData[UnsanitizedColumnId, t.Any]

class UnsanitizedTable(t.NamedTuple):
    schema: UnsanitizedTableSchema
    schema_checksum: str
    data: UnsanitizedTableData
    data_checksum: str

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

