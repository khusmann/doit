from __future__ import annotations
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

class SanitizedTableRepoReader(t.NamedTuple):
    read_table: t.Callable[[str], SanitizedTable]

class SanitizedTableRepoWriter(t.NamedTuple):
    write_table: t.Callable[[SanitizedTable, str], None]