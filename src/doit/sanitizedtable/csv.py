import csv
import hashlib

from ..common import (
    omitted_if_empty,
)

from .model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRowView,
    SanitizedTable,
)

def load_sanitizedtable_csv(csv_text: str) -> SanitizedTable:
    csv_lines = csv_text.splitlines()

    reader = csv.reader(csv_lines)

    column_ids = tuple(SanitizedColumnId(v) for v in next(reader))

    rows = tuple(
        SanitizedTableRowView(
            { h: omitted_if_empty(v) for h, v in zip(column_ids, row)}
        ) for row in reader
    )

    return SanitizedTable(
        info=SanitizedTableInfo(
            data_checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
            schema_checksum=hashlib.sha256(csv_lines[0].encode()).hexdigest(),
            columns=tuple(
                SanitizedColumnInfo(
                    id=cid,
                    prompt=cid.name,
                    sanitizer_checksum=None,
                    type='text',
                ) for cid in column_ids
            ),

        ),
        data=SanitizedTableData(
            column_ids=column_ids,
            rows=rows,
        )
    )
    
