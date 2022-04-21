import typing as t
from dataclasses import dataclass

from ..common import (
    TableRowView,
    RowViewHash,
    Error,
    Omitted,
)

from ..unsanitizedtable.model import UnsanitizedColumnId, UnsanitizedStrTableRowView
from ..sanitizedtable.model import SanitizedColumnId, SanitizedStrTableRowView

@dataclass(frozen=True)
class Sanitizer:
    _map: t.Mapping[RowViewHash[UnsanitizedColumnId, str], SanitizedStrTableRowView]
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    checksum: str

    def get(self, row: UnsanitizedStrTableRowView) -> SanitizedStrTableRowView:
        
        # If all row keys are Missing, return Missing for all new vals
        if (all(isinstance(v, Omitted) for v in row.values())):
            return TableRowView({ k: Omitted() for k in self.new_col_ids })

        # If any row keys are Error, return that Error for all new vals
        error = next((v for v in row.values() if isinstance(v, Error)), None)
        if error:
            return TableRowView({ k: error for k in self.new_col_ids })

        # Otherwise, look up the hash in the map
        return self._map.get(row.hash(), TableRowView(
            { k: Error('missing_sanitizer', row) for k in self.new_col_ids }
        ))