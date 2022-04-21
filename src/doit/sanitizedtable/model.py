import typing as t
from dataclasses import dataclass

from ..common import (
    ImmutableBaseModel,
    TableData,
    TableRowView,
)

class SanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedColumnInfo(ImmutableBaseModel):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: t.Optional[str]

class SanitizedTableInfo(ImmutableBaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SanitizedColumnInfo, ...]

SanitizedStrTableRowView = TableRowView[SanitizedColumnId, str]
SanitizedTableRowView = TableRowView[SanitizedColumnId, str]
SanitizedTableData = TableData[SanitizedColumnId, t.Any]

@dataclass(frozen=True)
class SanitizedTable:
    info: SanitizedTableInfo
    data: SanitizedTableData

