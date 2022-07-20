import typing as t

from doit.common.table import TableValue

from ..sanitizedtable.model import (
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedColumnId,
    LinkedTableRowView,
)

LinkFn = t.Callable[[SanitizedTableRowView], t.Tuple[LinkedColumnId, TableValue[t.Any]]]
AggregateFn = t.Callable[[LinkedTableRowView], t.Tuple[LinkedColumnId, TableValue[t.Any]]]

ExcludeFilterFn = t.Callable[[SanitizedTableRowView], bool]

class Linker(t.NamedTuple):
    dst_col_id: LinkedColumnId
    dst_col_type: t.Literal['ordinal', 'categorical', 'index', 'real', 'integer', 'multiselect', 'text']
    link_fn: LinkFn

class Aggregator(t.NamedTuple):
    linked_id: LinkedColumnId
    aggregate_fn: AggregateFn

class InstrumentLinker(t.NamedTuple):
    instrument_name: str
    exclude_filters: t.Tuple[ExcludeFilterFn, ...]
    linkers: t.Tuple[Linker, ...]
    aggregators: t.Tuple[Aggregator, ...]