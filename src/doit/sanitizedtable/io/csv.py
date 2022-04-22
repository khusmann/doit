import csv

from ...common import (
    omitted_if_empty,
)

from ..model import (
    SanitizedColumnId,
    SanitizedTableData,
    SanitizedTableRowView,
)

def load_sanitized_table_csv(csv_text: str) -> SanitizedTableData:
    reader = csv.reader(csv_text.splitlines())
    
    header = tuple(SanitizedColumnId(v) for v in next(reader))

    rows = tuple(
        SanitizedTableRowView(
            { h: omitted_if_empty(v) for h, v in zip(header, row)}
        ) for row in reader
    )

    return SanitizedTableData(
        column_ids=header,
        rows=rows,
    )