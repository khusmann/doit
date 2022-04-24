from __future__ import annotations
from abc import ABC
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

class SanitizedTableRepoReader(ABC):
    def read_table_info(self, name: str) -> SanitizedTableInfo: ...

class SanitizedTableRepoWriter(ABC):
    def write_table(self, table: SanitizedTable, name: str) -> None: ...