import typing as t

from ..common import (
    TableRowView,
    RowViewHash,
    Some,
    Error,
    Missing,
)

from ..unsanitizedtable.model import UnsanitizedColumnId, UnsanitizedStrTableRowView
from ..sanitizedtable.model import SanitizedColumnId, SanitizedStrTableRowView

class Sanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    map: t.Mapping[RowViewHash[UnsanitizedColumnId, str], SanitizedStrTableRowView]
    checksum: str
    def get(self, row: UnsanitizedStrTableRowView) -> SanitizedStrTableRowView:
        if (all(isinstance(v, Missing) for v in row.values())):
            return TableRowView({ k: Missing('omitted') for k in self.new_col_ids })
        match row.hash():
            case Some(value):
                return self.map.get(value, TableRowView(
                    { k: Error('missing_sanitizer', row) for k in self.new_col_ids }
                ))
            case Error() as e:
                return TableRowView({ k: e for k in self.new_col_ids })

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]
    checksum: str

