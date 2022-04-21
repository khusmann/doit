import csv
import re
import hashlib

from ...common import (
    Some,
    Missing
)

from ..model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedTableData,
    UnsanitizedTableRowView,
    UnsanitizedTable,
    UnsanitizedTableInfo,
)

class EmptyHeaderError(ValueError):
    pass

class DuplicateHeaderError(ValueError):
    pass

def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def load_unsanitized_table_csv(csv_text: str) -> UnsanitizedTable:
    reader = csv.reader(csv_text.splitlines())

    header = tuple(next(reader))

    if not all(header):
        raise EmptyHeaderError(header)

    if len(set(header)) != len(header):
        raise DuplicateHeaderError(header)

    columns = tuple(
        UnsanitizedColumnInfo(
            id=UnsanitizedColumnId(v if is_header_safe(v) else rename_unsafe_header(v)),
            prompt=v,
            type='text',
            is_safe=is_header_safe(v),
        ) for v in header
    )

    column_ids = tuple(c.id for c in columns)

    rows = tuple(
        UnsanitizedTableRowView(
            { cid: Some(v) if v else Missing('omitted') for cid, v in zip(column_ids, row)}
        ) for row in reader
    )

    return UnsanitizedTable(
        info=UnsanitizedTableInfo(
            data_checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
            schema_checksum=hashlib.sha256(csv_text[0].encode()).hexdigest(),
            columns=columns,
        ),
        data=UnsanitizedTableData(
            columns_ids=column_ids,
            rows=rows,
        ),
    )