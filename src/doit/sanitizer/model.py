import typing as t

from ..common import (
    TableRowView,
    RowViewHash,
    MaybeRowViewHash,
    Some,
    Missing,
    Error
)

from ..unsanitizedtable.model import UnsanitizedColumnId
from ..sanitizedtable.model import SanitizedColumnId, SanitizedStrTableRowView

class Sanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    map: t.Mapping[RowViewHash, SanitizedStrTableRowView]
    checksum: str
    def get(self, h: MaybeRowViewHash) -> SanitizedStrTableRowView:
        match h:
            case Some():
                return self.map.get(h.value, TableRowView(
                    { k: Missing('redacted') for k in self.new_col_ids }
                ))
            case Error():
                return TableRowView({ k: h for k in self.new_col_ids })

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]
    checksum: str

