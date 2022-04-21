import csv
import re
import hashlib

from ...common import (
    Some,
    Missing,
    DuplicateHeaderError,
    EmptyHeaderError,
    EmptySanitizerKeyError,
)

from ...unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedStrTableRowView,
)

from ...sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedStrTableRowView,
)

from ..model import (
    SanitizedColumnId,
    Sanitizer,
)


def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def load_sanitizer_csv(csv_text: str) -> Sanitizer:
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
        UnsanitizedStrTableRowView({
            key_col_names[c]: Some(v) if v else Missing('omitted')
                for c, v in zip(header, row) if c in key_col_names
        }) for row in lines
    )

    values = tuple(
        SanitizedStrTableRowView({
            new_col_names[c]: Some(v) if v else Missing('redacted')
                for c, v in zip(header, row)if c in new_col_names
        }) for row in lines
    )


    for key, value in zip(keys, values):
        # Insure key columns have at least one real value
        if not any(isinstance(k, Some) for k in key.values()):
            raise EmptySanitizerKeyError(tuple(v for v in value.values()))

    hash_map = {
        key.hash(): new
            for key, new in zip(keys, values)
    }

    return Sanitizer(
        map=hash_map,
        key_col_ids=tuple(key_col_names.values()),
        new_col_ids=tuple(new_col_names.values()),
        checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
    )