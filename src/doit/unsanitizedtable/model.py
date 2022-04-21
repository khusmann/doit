import typing as t

from ..common import (
    TableData,
    TableRowView,
)

class UnsanitizedColumnId(t.NamedTuple):
    unsafe_name: str

class UnsanitizedColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    type: t.Literal['text', 'bool', 'ordinal']
    is_safe: bool

class UnsanitizedTableInfo(t.NamedTuple):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[UnsanitizedColumnInfo, ...]

UnsanitizedStrTableRowView = TableRowView[UnsanitizedColumnId, str]
UnsanitizedTableRowView = TableRowView[UnsanitizedColumnId, t.Any]

UnsanitizedTableData = TableData[UnsanitizedColumnId, t.Any]

class UnsanitizedTable(t.NamedTuple):
    info: UnsanitizedTableInfo
    data: UnsanitizedTableData

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

