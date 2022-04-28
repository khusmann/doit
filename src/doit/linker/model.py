import typing as t

from ..common.table import (
    TableValue
)

from ..sanitizedtable.model import (
    SanitizedTableRowView,
)

from ..study.model import (
    LinkedColumnId,
    LinkedTableRowView,
)

FromFn = t.Callable[[SanitizedTableRowView], TableValue]
ToFn = t.Callable[[TableValue], LinkedTableRowView]

class Linker(t.NamedTuple):
    from_src: FromFn
    to_dst: ToFn
    dst_col_ids: t.Tuple[LinkedColumnId, ...]