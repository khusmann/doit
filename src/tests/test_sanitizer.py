import pytest
from textwrap import dedent
from itertools import repeat

from doit.common.table import (
    EmptySanitizerKeyError,
    Omitted,
    Redacted,
    Some,
    ColumnNotFoundInRow,
    DuplicateHeaderError,
    EmptyHeaderError,
    ErrorValue
)

from doit.unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTableRowView,
)

from doit.sanitizer.io import (
    load_sanitizer_csv,
)

from doit.service.sanitize import (
    sanitize_row,
)

def test_basic_load():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,6,11
        7,8,9,12
    """)

    testrow = UnsanitizedTableRowView((
        (UnsanitizedColumnId("b"), Some("5")),
        (UnsanitizedColumnId("d"), Some("11")),
    ))

    testrow_reverse = UnsanitizedTableRowView((
        (UnsanitizedColumnId("d"), Some("11")),
        (UnsanitizedColumnId("b"), Some("5")),
    ))

    sanitizer = load_sanitizer_csv(raw, "test_sanitizer")
    
    assert [c.unsafe_name for c in sanitizer.key_col_ids] == ["b", "d"]
    assert [c.name for c in sanitizer.new_col_ids] == ["a", "c"]

    assert list(v for _, v in sanitize_row(testrow, sanitizer)) == [Some("4"), Some("6")]
    assert list(v for _, v in sanitize_row(testrow_reverse, sanitizer)) == [Some("4"), Some("6")]

def test_missing_load():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,,9,12
    """)

    testrow = UnsanitizedTableRowView((
        (UnsanitizedColumnId("b"), Some("5")),
        (UnsanitizedColumnId("d"), Some("11")),
    ))

    testrow_missing = UnsanitizedTableRowView((
        (UnsanitizedColumnId("b"), Omitted()),
        (UnsanitizedColumnId("d"), Some("12")),
    ))

    testrow_error = UnsanitizedTableRowView((
        (UnsanitizedColumnId("z"), Some("10")),
    ))

    sanitizer = load_sanitizer_csv(raw, "test_sanitizer")

    assert [c.unsafe_name for c in sanitizer.key_col_ids] == ["b", "d"]
    assert [c.name for c in sanitizer.new_col_ids] == ["a", "c"]

    assert list(v for _, v in sanitize_row(testrow, sanitizer)) == [Some("4"), Redacted()]
    assert list(v for _, v in sanitize_row(testrow_missing, sanitizer)) == [Some("7"), Some("9")]
    assert list(v for _, v in sanitize_row(testrow_error, sanitizer)) == list(repeat(ErrorValue(ColumnNotFoundInRow(UnsanitizedColumnId('b'), testrow_error)), 2))

def test_missing_keys():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,8,9,
    """)

    testrow = UnsanitizedTableRowView((
        (UnsanitizedColumnId("b"), Some("8")),
        (UnsanitizedColumnId("d"), Omitted()),
    ))

    testrow_allmissing = UnsanitizedTableRowView((
        (UnsanitizedColumnId("b"), Omitted()),
        (UnsanitizedColumnId("d"), Omitted()),
    ))

    sanitizer = load_sanitizer_csv(raw, "test_sanitizer")

    assert list(v for _, v in sanitize_row(testrow, sanitizer)) == [Some("7"), Some("9")]
    assert list(v for _, v in sanitize_row(testrow_allmissing, sanitizer)) == [Omitted(), Omitted()]

def test_missing_key_error():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,,9,
    """)

    with pytest.raises(EmptySanitizerKeyError):
        load_sanitizer_csv(raw, "test_sanitizer")

def test_missing_header_error():
    raw = dedent("""\
        a,b,,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(EmptyHeaderError):
        load_sanitizer_csv(raw, "test_sanitizer")

def test_duplicate_header_error():
    raw = dedent("""\
        a,a,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(DuplicateHeaderError):
        load_sanitizer_csv(raw, "test_sanitizer")