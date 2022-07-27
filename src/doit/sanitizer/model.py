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
    name: str
    sources: t.Mapping[str, t.Tuple[str, ...]]
    remote_ids: t.Tuple[str, ...]
    prompt: t.Optional[str]
    map: t.Mapping[t.Tuple[TableValue[t.Any], ...], t.Tuple[TableValue[t.Any], ...]]

    def key_col_ids(self, table_name: str):
        return tuple(UnsanitizedColumnId(i) for i in self.sources.get(table_name, ()))

    @property
    def new_col_ids(self):
        return (SanitizedColumnId(i) for i in self.remote_ids)

class OmitSanitizer(t.NamedTuple):
    name: str
    source: str
    prompt: str
    remote_id: str

    def key_col_ids(self, table_name: str) -> t.Tuple[UnsanitizedColumnId, ...]:
        return (UnsanitizedColumnId(self.remote_id),) if self.source == table_name else ()

    @property
    def new_col_ids(self) -> t.Tuple[SanitizedColumnId, ...]:
        return (SanitizedColumnId(self.remote_id),)

class IdentitySanitizer(t.NamedTuple):
    name: str
    source: str
    prompt: str
    remote_id: str

    def key_col_ids(self, table_name: str) -> t.Tuple[UnsanitizedColumnId, ...]:
        return (UnsanitizedColumnId(self.remote_id),) if self.source == table_name else ()

    @property
    def new_col_ids(self) -> t.Tuple[SanitizedColumnId, ...]:
        return (SanitizedColumnId(self.remote_id),)

RowSanitizer = LookupSanitizer | IdentitySanitizer | OmitSanitizer