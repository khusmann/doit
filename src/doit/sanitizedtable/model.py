import typing as t

from ..common.table import (
    TableData,
    TableRowView,
)

class SanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedSimpleColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: t.Optional[str]
    value_type: t.Literal['text'] = 'text'

class SanitizedCodedColumnInfo(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    codes: t.Mapping[int, str]
    value_type: t.Literal['ordinal', 'multiselect']

SanitizedColumnInfo = SanitizedSimpleColumnInfo | SanitizedCodedColumnInfo

class SanitizedTableInfo(t.NamedTuple):
    name: str
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SanitizedColumnInfo, ...]

SanitizedTableRowView = TableRowView[SanitizedColumnId]
SanitizedTableData = TableData[SanitizedColumnId]

class SanitizedTable(t.NamedTuple):
    info: SanitizedTableInfo
    data: SanitizedTableData