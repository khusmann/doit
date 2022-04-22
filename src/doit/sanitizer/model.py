import typing as t

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTableRowView,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedTableRowView
)

class LookupSanitizer(t.NamedTuple):
    map: t.Mapping[UnsanitizedTableRowView, SanitizedTableRowView]
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    checksum: str

class IdentitySanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    @property
    def new_col_ids(self):
        return tuple(SanitizedColumnId(i.unsafe_name) for i in self.key_col_ids)

Sanitizer = LookupSanitizer | IdentitySanitizer

# TODO
# class MultiselectSanitizer:
# map a multiselect column into multiple bool columns...

# vs. LookupSanitizer

# Sanitizer = LookupSanitier | MultiselectSanitizer

# def sanitize_row(sanitizer: Sanitizer, row: UnsanitizedTableRowView[t.Any]) -> SanitizedTableRowView[t.Any]: