import typing as t

from ..common.table import (
    TableData,
    TableRowView,
)

class UnsanitizedColumnId(t.NamedTuple):
    unsafe_name: str

class UnsanitizedTextColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    is_safe: bool
    value_type: t.Literal['text'] = 'text'

class UnsanitizedOrdinalColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    codes: t.Mapping[int, str]
    is_safe = True
    value_type: t.Literal['ordinal', 'multiselect']

UnsanitizedColumnInfo = UnsanitizedTextColumnInfo | UnsanitizedOrdinalColumnInfo

UnsanitizedTableRowView = TableRowView[UnsanitizedColumnId]
UnsanitizedTableData = TableData[UnsanitizedColumnId]

class UnsanitizedTable(t.NamedTuple):
    schema: t.Tuple[UnsanitizedColumnInfo, ...]
    schema_checksum: str
    data: UnsanitizedTableData
    data_checksum: str
    source_name: str
    source_title: str

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

