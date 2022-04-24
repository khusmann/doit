import pytest
from textwrap import dedent

from doit.common import (
    Some,
    Omitted,
    DuplicateHeaderError,
    EmptyHeaderError,
)

from doit.unsanitizedtable.impl.csv import (
    load_unsanitizedtable_csv,
)

def test_basic_load():
    raw = dedent("""\
        a,(b),c
        1,2,3
        4,5,6
        7,8,9
    """)

    table = load_unsanitizedtable_csv(raw, "CSV Import")

    assert [c.id.unsafe_name for c in table.schema] == ["a", "b", "c"]
    assert [c.is_safe for c in table.schema] == [True, False, True]

    assert [c.unsafe_name for c in table.data.column_ids] == ["a", "b", "c"]
    assert [c.unsafe_name for c in table.data.rows[1].column_ids()] == ["a", "b", "c"]
    assert [c for c in table.data.rows[1].values()] == [Some("4"), Some("5"), Some("6")]

def test_missing_load():
    raw = dedent("""\
        a,b,c
        1,2,3
        4,5,
        7,8,9
    """)

    table = load_unsanitizedtable_csv(raw, "CSV Import")

    assert [c.unsafe_name for c in table.data.column_ids] == ["a", "b", "c"]
    assert [c.unsafe_name for c in table.data.rows[1].column_ids()] == ["a", "b", "c"]
    assert [c for c in table.data.rows[1].values()] == [Some("4"), Some("5"), Omitted()]

def test_missing_header_error():
    raw = dedent("""\
        a,b,,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(EmptyHeaderError):
        load_unsanitizedtable_csv(raw, "CSV Import")

def test_duplicate_header_error():
    raw = dedent("""\
        a,a,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(DuplicateHeaderError):
        load_unsanitizedtable_csv(raw, "CSV Import")