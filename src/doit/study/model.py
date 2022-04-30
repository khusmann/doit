import typing as t

from ..common.table import (
    TableData,
    TableRowView,
)

### LinkedTable

class LinkedColumnId(t.NamedTuple):
    linked_name: str

LinkedTableData = TableData[LinkedColumnId]
LinkedTableRowView = TableRowView[LinkedColumnId]

class LinkedTable(t.NamedTuple):
    studytable_name: str
    data: LinkedTableData