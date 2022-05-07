import csv
import hashlib

from ..common.table import (
    Some,
    Omitted,
)

from .model import (
    SanitizedColumnId,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRowView,
    SanitizedTable,
    SanitizedSimpleColumnInfo,
)

def load_sanitizedtable_csv(csv_text: str, name: str) -> SanitizedTable:
    csv_lines = csv_text.splitlines()

    reader = csv.reader(csv_lines)

    column_ids = tuple(SanitizedColumnId(v) for v in next(reader))

    rows = tuple(
        SanitizedTableRowView(
            (h, Some(v) if v else Omitted()) for h, v in zip(column_ids, row)
        ) for row in reader
    )

    return SanitizedTable(
        info=SanitizedTableInfo(
            name=name,
            title=name,
            data_checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
            schema_checksum=hashlib.sha256(csv_lines[0].encode()).hexdigest(),
            columns=tuple(
                SanitizedSimpleColumnInfo(
                    id=cid,
                    prompt=cid.name,
                    sanitizer_checksum=None,
                    sortkey=str(i),
                ) for i, cid in enumerate(column_ids)
            ),

        ),
        data=SanitizedTableData(
            column_ids=column_ids,
            rows=rows,
        )
    )
    
