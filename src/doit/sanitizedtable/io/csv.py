import csv

from ...common import (
    Some,
    Missing
)

from ..model import (
    SanitizedColumnId,
    SanitizedTableData,
    SanitizedTableRowView,
)

def load_unsanitized_table_csv(csv_text: str) -> SanitizedTableData:
    reader = csv.reader(csv_text.splitlines())
    
    header = tuple(SanitizedColumnId(v) for v in next(reader))

    rows = tuple(
        SanitizedTableRowView(
            { h: Some(v) if v else Missing('omitted') for h, v in zip(header, row)}
        ) for row in reader
    )

    return SanitizedTableData(
        columns_ids=header,
        rows=rows,
    )