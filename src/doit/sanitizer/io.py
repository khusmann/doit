import csv
import re
import hashlib
import io

from ..common.table import (
    Omitted,
    Some,
    DuplicateHeaderError,
    EmptyHeaderError,
    EmptySanitizerKeyError,
    TableValue,
    omitted_if_empty,
    redacted_if_empty,
    TableRowView,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
)

from .model import (
    SanitizedColumnId,
    LookupSanitizer,
    SanitizerUpdate,
)

def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def to_csv_header(cid: SanitizedColumnId | UnsanitizedColumnId):
    match cid:
        case SanitizedColumnId():
            return cid.name
        case UnsanitizedColumnId():
            return "({})".format(cid.unsafe_name)

def to_csv_value(tv: TableValue):
    match tv:
        case Some(value=value) if isinstance(value, str):
            return value
        case Omitted():
            return ""
        case _:
            raise Exception("Error: cannot convert {} to csv value".format(tv))
            
def write_sanitizer_update(f: io.TextIOBase, update: SanitizerUpdate, new: bool):
    writer = csv.writer(f)

    if new:
        writer.writerow((to_csv_header(cid) for cid in update.header))

    writer.writerows((
        (
            to_csv_value(row.get(cid)) if isinstance(cid, UnsanitizedColumnId) else ""
                for cid in update.header
        ) for row in update.rows
    ))

def load_sanitizer_csv(csv_text: str) -> LookupSanitizer:
    reader = csv.reader(io.StringIO(csv_text, newline=''))

    header_str = tuple(next(reader))

    lines = tuple(reader)

    if not all(header_str):
        raise EmptyHeaderError(header_str)

    if len(set(header_str)) != len(header_str):
        raise DuplicateHeaderError(header_str)

    header = tuple(
        SanitizedColumnId(c) if is_header_safe(c) else UnsanitizedColumnId(rename_unsafe_header(c))
            for c in header_str
    )

    keys = tuple(
        TableRowView({
            c: omitted_if_empty(v)
                for c, v in zip(header, row) if isinstance(c, UnsanitizedColumnId)
        }) for row in lines
    )

    values = tuple(
        TableRowView({
            c: redacted_if_empty(v)
                for c, v in zip(header, row)if isinstance(c, SanitizedColumnId)
        }) for row in lines
    )


    for key, value in zip(keys, values):
        # Insure key columns have at least one real value
        if not any(isinstance(k, Some) for k in key.values()):
            raise EmptySanitizerKeyError(tuple(v for v in value.values()))

    return LookupSanitizer(
        map=dict(zip(keys, values)),
        header=header,
        key_col_ids=tuple(c for c in header if isinstance(c, UnsanitizedColumnId)),
        new_col_ids=tuple(c for c in header if isinstance(c, SanitizedColumnId)),
        checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
    )