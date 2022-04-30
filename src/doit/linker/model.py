import typing as t

from ..sanitizedtable.model import (
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedColumnId,
    LinkedTableRowView,
)

class Linker(t.NamedTuple):
    dst_col_ids: t.Tuple[LinkedColumnId, ...]
    link_fn: t.Callable[[SanitizedTableRowView], LinkedTableRowView]

class InstrumentLinker(t.NamedTuple):
    studytable_name: str
    instrument_name: str
    linkers: t.Tuple[Linker, ...]