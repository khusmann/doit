import typing as t

from ..common import (
    TableData,
    TableRowView,
    OrdinalLabel,
    OrdinalValue,
)

class UnsanitizedColumnId(t.NamedTuple):
    unsafe_name: str

class UnsanitizedTextColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    is_safe: bool

class UnsanitizedMultiselectColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    codes: t.Mapping[OrdinalValue, OrdinalLabel]
    is_safe = True

class UnsanitizedOrdinalColumnInfo(t.NamedTuple):
    id: UnsanitizedColumnId
    prompt: str
    codes: t.Mapping[OrdinalValue, OrdinalLabel]
    is_safe = True

UnsanitizedColumnInfo = UnsanitizedTextColumnInfo | UnsanitizedMultiselectColumnInfo | UnsanitizedOrdinalColumnInfo

UnsanitizedTableRowView = TableRowView[UnsanitizedColumnId, t.Any]

UnsanitizedTableData = TableData[UnsanitizedColumnId, t.Any]

class UnsanitizedTable(t.NamedTuple):
    schema: t.Tuple[UnsanitizedColumnInfo, ...]
    schema_checksum: str
    data: UnsanitizedTableData
    data_checksum: str

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

