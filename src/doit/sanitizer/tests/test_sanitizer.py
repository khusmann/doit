import pytest
from textwrap import dedent

from doit.common import (
    EmptySanitizerKeyError,
    Some,
    Missing,
    Error,
    DuplicateHeaderError,
    EmptyHeaderError,
)

from doit.unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedStrTableRowView,
)

from doit.sanitizer.io.csv import (
    load_sanitizer_csv,
)

def test_basic_load():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,6,11
        7,8,9,12
    """)

    testrow = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("b"): Some("5"),
        UnsanitizedColumnId("d"): Some("11"),
    })

    testrow_reverse = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("d"): Some("11"),
        UnsanitizedColumnId("b"): Some("5"),
    })

    sanitizer = load_sanitizer_csv(raw)
    
    assert [c.unsafe_name for c in sanitizer.key_col_ids] == ["b", "d"]
    assert [c.name for c in sanitizer.new_col_ids] == ["a", "c"]

    assert list(sanitizer.get(testrow.hash()).values()) == [Some("4"), Some("6")]
    assert list(sanitizer.get(testrow_reverse.hash()).values()) == [Some("4"), Some("6")]

def test_missing_load():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,,9,12
    """)

    testrow = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("b"): Some("5"),
        UnsanitizedColumnId("d"): Some("11"),
    })

    testrow_missing = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("b"): Missing('omitted'),
        UnsanitizedColumnId("d"): Some("12"),
    })

    testrow_error = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("z"): Some("10")
    })

    sanitizer = load_sanitizer_csv(raw)

    assert [c.unsafe_name for c in sanitizer.key_col_ids] == ["b", "d"]
    assert [c.name for c in sanitizer.new_col_ids] == ["a", "c"]

    assert list(sanitizer.get(testrow.hash()).values()) == [Some("4"), Missing('redacted')]
    assert list(sanitizer.get(testrow_missing.hash()).values()) == [Some("7"), Some("9")]
    assert list(sanitizer.get(testrow_error.hash()).values()) == [Error('missing_sanitizer'), Error("missing_sanitizer")]


def test_missing_keys():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,8,9,
    """)

    testrow = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("b"): Some("8"),
        UnsanitizedColumnId("d"): Missing('omitted')
    })

    testrow_allmissing = UnsanitizedStrTableRowView({
        UnsanitizedColumnId("b"): Missing('omitted'),
        UnsanitizedColumnId("d"): Missing('omitted')
    })

    sanitizer = load_sanitizer_csv(raw)

    assert list(sanitizer.get(testrow.hash()).values()) == [Some("7"), Some("9")]
    assert list(sanitizer.get(testrow_allmissing.hash()).values()) == [Missing('omitted'), Missing('omitted')]

def test_missing_key_error():
    raw = dedent("""\
        a,(b),c,(d)
        1,2,3,10
        4,5,,11
        7,,9,
    """)

    with pytest.raises(EmptySanitizerKeyError):
        load_sanitizer_csv(raw)

def test_missing_header_error():
    raw = dedent("""\
        a,b,,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(EmptyHeaderError):
        load_sanitizer_csv(raw)

def test_duplicate_header_error():
    raw = dedent("""\
        a,a,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(DuplicateHeaderError):
        load_sanitizer_csv(raw)