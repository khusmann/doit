import csv
import re
import hashlib

from ...common.table import (
    Some,
    DuplicateHeaderError,
    EmptyHeaderError,
    EmptySanitizerKeyError,
    omitted_if_empty,
    redacted_if_empty,
    TableRowView,
)

from ...unsanitizedtable.model import (
    UnsanitizedColumnId,
)

from ...sanitizedtable.model import (
    SanitizedColumnId,
)

from ..model import (
    SanitizedColumnId,
    LookupSanitizer,
)

def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def load_sanitizer_csv(csv_text: str) -> LookupSanitizer:
    reader = csv.reader(csv_text.splitlines())

    header = tuple(next(reader))

    lines = tuple(reader)

    if not all(header):
        raise EmptyHeaderError(header)

    if len(set(header)) != len(header):
        raise DuplicateHeaderError(header)

    key_col_names = {c: UnsanitizedColumnId(rename_unsafe_header(c)) for c in header if not is_header_safe(c)}
    new_col_names = {c: SanitizedColumnId(c) for c in header if c not in key_col_names}

    keys = tuple(
        TableRowView({
            key_col_names[c]: omitted_if_empty(v)
                for c, v in zip(header, row) if c in key_col_names
        }) for row in lines
    )

    values = tuple(
        TableRowView({
            new_col_names[c]: redacted_if_empty(v)
                for c, v in zip(header, row)if c in new_col_names
        }) for row in lines
    )


    for key, value in zip(keys, values):
        # Insure key columns have at least one real value
        if not any(isinstance(k, Some) for k in key.values()):
            raise EmptySanitizerKeyError(tuple(v for v in value.values()))

    return LookupSanitizer(
        map=dict(zip(keys, values)),
        key_col_ids=tuple(key_col_names.values()),
        new_col_ids=tuple(new_col_names.values()),
        checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
    )