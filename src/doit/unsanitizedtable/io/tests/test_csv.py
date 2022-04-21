import pytest
from textwrap import dedent

from ....common import Some, Missing
from ..csv import (
    load_unsanitized_table_csv,
    DuplicateHeaderError,
    EmptyHeaderError,
)

def test_basic_load():
    raw = dedent("""\
        a,(b),c
        1,2,3
        4,5,6
        7,8,9
    """)

    table = load_unsanitized_table_csv(raw)

    assert [c.id.unsafe_name for c in table.info.columns] == ["a", "b", "c"]
    assert [c.is_safe for c in table.info.columns] == [True, False, True]

    assert [c.unsafe_name for c in table.data.columns_ids] == ["a", "b", "c"]
    assert [c.unsafe_name for c in table.data.rows[1].keys()] == ["a", "b", "c"]
    assert [c for c in table.data.rows[1].values()] == [Some("4"), Some("5"), Some("6")]

def test_missing_load():
    raw = dedent("""\
        a,b,c
        1,2,3
        4,5,
        7,8,9
    """)

    table = load_unsanitized_table_csv(raw)

    assert [c.unsafe_name for c in table.data.columns_ids] == ["a", "b", "c"]
    assert [c.unsafe_name for c in table.data.rows[1].keys()] == ["a", "b", "c"]
    assert [c for c in table.data.rows[1].values()] == [Some("4"), Some("5"), Missing('omitted')]

def test_missing_header_error():
    raw = dedent("""\
        a,b,,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(EmptyHeaderError):
        load_unsanitized_table_csv(raw)

def test_duplicate_header_error():
    raw = dedent("""\
        a,a,c
        1,2,3
        4,5,6
        7,8,9
    """)

    with pytest.raises(DuplicateHeaderError):
        load_unsanitized_table_csv(raw)