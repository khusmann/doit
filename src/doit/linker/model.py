import typing as t

from doit.common.table import TableValue

from ..sanitizedtable.model import (
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedColumnId,
)

class Linker(t.NamedTuple):
    dst_col_id: LinkedColumnId
    dst_col_type: t.Literal['ordinal', 'categorical', 'index', 'real', 'integer', 'multiselect', 'text']
    link_fn: t.Callable[[SanitizedTableRowView], t.Tuple[LinkedColumnId, TableValue[t.Any]]]

class InstrumentLinker(t.NamedTuple):
    studytable_name: str
    instrument_name: str
    linkers: t.Tuple[Linker, ...]