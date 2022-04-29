import typing as t

from ..common.table import TableValue

from ..sanitizedtable.model import (
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedColumnId,
)

class Linker(t.NamedTuple):
    dst_col_ids: t.Tuple[LinkedColumnId, ...]
    link_fn: t.Callable[[SanitizedTableRowView], t.Tuple[t.Tuple[LinkedColumnId, TableValue], ...]]

class InstrumentLinker(t.NamedTuple):
    studytable_name: str
    linkers: t.Tuple[Linker, ...]