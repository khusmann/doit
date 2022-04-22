import csv
import re
import hashlib

from ...common import (
    Some,
    Omitted,
    DuplicateHeaderError,
    EmptyHeaderError,
)

from ..model import (
    UnsanitizedColumnId,
    UnsanitizedTextColumnInfo,
    UnsanitizedTableData,
    UnsanitizedTableRowView,
    UnsanitizedTable,
    UnsanitizedTableSchema,
)

def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def load_unsanitizedtable_csv(csv_text: str) -> UnsanitizedTable:
    reader = csv.reader(csv_text.splitlines())

    header = tuple(next(reader))

    if not all(header):
        raise EmptyHeaderError(header)

    if len(set(header)) != len(header):
        raise DuplicateHeaderError(header)

    columns = tuple(
        UnsanitizedTextColumnInfo(
            id=UnsanitizedColumnId(v if is_header_safe(v) else rename_unsafe_header(v)),
            prompt=rename_unsafe_header(v),
            is_safe=is_header_safe(v),
        ) for v in header
    )

    column_ids = tuple(c.id for c in columns)

    rows = tuple(
        UnsanitizedTableRowView(
            { cid: Some(v) if v else Omitted() for cid, v in zip(column_ids, row)}
        ) for row in reader
    )

    return UnsanitizedTable(
        schema=UnsanitizedTableSchema(
            columns=columns,
        ),
        data=UnsanitizedTableData(
            column_ids=column_ids,
            rows=rows,
        ),
        data_checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
        schema_checksum=hashlib.sha256(csv_text[0].encode()).hexdigest(),
    )