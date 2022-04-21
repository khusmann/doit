import typing as t
from dataclasses import dataclass

from ..common import (
    ImmutableBaseModel,
    TableData,
    TableRowView,
)

class UnsanitizedColumnId(t.NamedTuple):
    unsafe_name: str

class UnsanitizedColumnInfo(ImmutableBaseModel):
    id: UnsanitizedColumnId
    prompt: str
    type: t.Literal['text', 'bool', 'ordinal']
    is_safe: bool

class UnsanitizedTableInfo(ImmutableBaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[UnsanitizedColumnInfo, ...]

UnsanitizedStrTableRowView = TableRowView[UnsanitizedColumnId, str]
UnsanitizedTableRowView = TableRowView[UnsanitizedColumnId, t.Any]

UnsanitizedTableData = TableData[UnsanitizedColumnId, t.Any]

@dataclass(frozen=True)
class UnsanitizedTable:
    info: UnsanitizedTableInfo
    data: UnsanitizedTableData

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

