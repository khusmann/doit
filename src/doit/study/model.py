import typing as t

from ..common.table import (
    TableData,
    TableRowView,
)

### LinkedTable

class LinkedColumnId(t.NamedTuple):
    linked_name: str

class LinkedColumnInfo(t.NamedTuple):
    id: LinkedColumnId
    value_type: t.Literal['text', 'real', 'integer', 'ordinal', 'multiselect', 'categorical', 'index']

LinkedTableData = TableData[LinkedColumnId]
LinkedTableRowView = TableRowView[LinkedColumnId]

class LinkedTable(t.NamedTuple):
    instrument_name: str
    columns: t.Tuple[LinkedColumnInfo, ...]    
    data: LinkedTableData