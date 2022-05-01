import typing as t

from ..common.table import (
    TableData,
    TableRowView,
)

class SanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedTextColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: t.Optional[str]

class SanitizedOrdinalColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    codes: t.Mapping[int, str]

class SanitizedMultiselectColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    codes: t.Mapping[int, str]

SanitizedColumnInfo = SanitizedTextColumnInfo | SanitizedOrdinalColumnInfo | SanitizedMultiselectColumnInfo

class SanitizedTableInfo(t.NamedTuple):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SanitizedColumnInfo, ...]

SanitizedTableRowView = TableRowView[SanitizedColumnId]
SanitizedTableData = TableData[SanitizedColumnId]

class SanitizedTable(t.NamedTuple):
    info: SanitizedTableInfo
    data: SanitizedTableData