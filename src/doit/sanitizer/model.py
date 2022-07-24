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
    name: str
    prompt: str
    map: t.Mapping[UnsanitizedTableRowView, t.Tuple[t.Tuple[SanitizedColumnId, TableValue[t.Any]], ...]]
    header: t.Tuple[UnsanitizedColumnId | SanitizedColumnId, ...]
    checksum: str

    @property
    def key_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, UnsanitizedColumnId))
    
    @property
    def new_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, SanitizedColumnId))

class OmitSanitizer(t.NamedTuple):
    name: str
    prompt: str

    @property
    def key_col_ids(sef):
        return ()

    @property
    def new_col_ids(self) -> t.Tuple[SanitizedColumnId, ...]:
        return ()

class IdentitySanitizer(t.NamedTuple):
    name: str
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    prompt: str

    @property
    def new_col_ids(self):
        return tuple(SanitizedColumnId(i.unsafe_name) for i in self.key_col_ids)

RowSanitizer = LookupSanitizer | IdentitySanitizer | OmitSanitizer

class TableSanitizer(t.NamedTuple):
    table_name: str
    sanitizers: t.Mapping[str, RowSanitizer]

class StudySanitizer(t.NamedTuple):
    table_sanitizers: t.Mapping[str, TableSanitizer]

class SanitizerUpdate(t.NamedTuple):
    table_name: str
    sanitizer_name: str
    header: t.Tuple[UnsanitizedColumnId | SanitizedColumnId, ...]
    rows: t.Tuple[UnsanitizedTableRowView, ...]

    @property
    def key_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, UnsanitizedColumnId))
    
    @property
    def new_col_ids(self):
        return tuple(c for c in self.header if isinstance(c, SanitizedColumnId))

