import typing as t

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
)

from ..common.table import (
    TableValue
)

class LookupSanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    prompt: str
    map: t.Mapping[t.Tuple[TableValue[t.Any], ...], t.Tuple[TableValue[t.Any], ...]]

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
    sanitizers: t.Tuple[RowSanitizer, ...]

class StudySanitizer(t.NamedTuple):
    table_sanitizers: t.Mapping[str, TableSanitizer]