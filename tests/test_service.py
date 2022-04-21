from __future__ import annotations
import pytest

from doit.common import (
    Some,
)

from doit.unsanitizedtable.model import (
    UnsanitizedTable,
    UnsanitizedTableInfo,
    UnsanitizedColumnInfo,
    UnsanitizedColumnId,
    UnsanitizedTableData,
    UnsanitizedTableRowView,
)

from doit.sanitizer.model import (
    SanitizerSpec,
)

from doit.service.sanitize import (
    sanitizer_from_spec,
    sanitize_table,
)

@pytest.fixture
def table() -> UnsanitizedTable:
    col1 = tuple(Some("<{}, 1>".format(v)) for v in range(5))
    col2 = tuple(Some("<{}, 2>".format(v)) for v in range(5))
    col3 = tuple(Some("<{}, 3>".format(v)) for v in range(5))

    info = UnsanitizedTableInfo(
        data_checksum="data_checksum",
        schema_checksum="schema_checksum",
        columns=tuple(
            UnsanitizedColumnInfo(
                id=UnsanitizedColumnId("col{}".format(c)),
                prompt="prompt {}".format(c),
                type="text",
                is_safe=True,
            ) for c in range(1, 4)
        )
    )

    return UnsanitizedTable(
        info=info,
        data=UnsanitizedTableData(
            columns=tuple(c.id for c in info.columns),
            rows=tuple(UnsanitizedTableRowView({ c.id: v for c, v in zip(info.columns, row)}) for row in zip(col1, col2, col3))
        ),
    ) 

@pytest.fixture
def san_table() -> SanitizerSpec:
    col1 = tuple("<{}, 1>".format(v) for v in range(5))
    col2 = (*tuple("<{}, sanitized>".format(v) for v in range(4)), "")

    colinfo1 = "(col1)"
    colinfo2 = "sanitized_col1"

    return SanitizerSpec(
        header=(colinfo1, colinfo2),
        rows=tuple(tuple(v) for v in zip(col1, col2)),
        checksum="sanitizer1 checksum",
    ) 

@pytest.fixture
def san_table2() -> SanitizerSpec:
    col1 = tuple("<{}, 3>".format(v) for v in range(5))
    col2 = tuple("<{}, sanitized3>".format(v) for v in range(5))

    colinfo1 = "(col3)"
    colinfo2 = "sanitized_col3"

    return SanitizerSpec(
        header=(colinfo1, colinfo2),
        rows=tuple(tuple(v) for v in zip(col1, col2)),
        checksum="sanitizer2 checksum",
    ) 

def test_rowwise(table: UnsanitizedTable):
    row = next(iter(table.data.rows))
    assert list(c.unsafe_name for c in row.map) == ['col1', 'col2', 'col3']

def test_hash(table: UnsanitizedTable, san_table: SanitizerSpec, san_table2: SanitizerSpec):
    sanitizer = sanitizer_from_spec(san_table)
    sanitizer2 = sanitizer_from_spec(san_table2)

    sanitized_table = sanitize_table(table, [sanitizer, sanitizer2])

    print(sanitized_table)

    assert 5==6



# Column names in () should be known to be private. (child_name) vs child_name