import typing as t

from doit.common.table import TableValue

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTableRowView,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
)

class LookupSanitizer(t.NamedTuple):
    map: t.Mapping[UnsanitizedTableRowView, t.Tuple[t.Tuple[SanitizedColumnId, TableValue], ...]]
    header: t.Tuple[UnsanitizedColumnId | SanitizedColumnId, ...]
    checksum: str

    @property
    def key_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, UnsanitizedColumnId))
    
    @property
    def new_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, SanitizedColumnId))


class IdentitySanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]

    @property
    def new_col_ids(self):
        return tuple(SanitizedColumnId(i.unsafe_name) for i in self.key_col_ids)

Sanitizer = LookupSanitizer | IdentitySanitizer

class SanitizerUpdate(t.NamedTuple):
    new: bool
    header: t.Tuple[UnsanitizedColumnId | SanitizedColumnId, ...]
    rows: t.Tuple[UnsanitizedTableRowView, ...]