import typing as t

from ..common import (
    TableData,
    TableRowView,
)

class SanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: t.Optional[str]

class SanitizedTableInfo(t.NamedTuple):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SanitizedColumnInfo, ...]

SanitizedTableRowView = TableRowView[SanitizedColumnId]
SanitizedTableData = TableData[SanitizedColumnId]

class SanitizedTable(t.NamedTuple):
    info: SanitizedTableInfo
    data: SanitizedTableData

