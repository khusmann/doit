from textwrap import dedent
from doit.sanitizedtable.model import SanitizedColumnId

from doit.common.table import (
    Redacted,
)

from doit.sanitizer.io import (
    load_sanitizer_csv,
)

from doit.unsanitizedtable.io.csv import (
    load_unsanitizedtable_csv,
)

from doit.sanitizedtable.io import (
    load_sanitizedtable_csv,
)

from doit.service.sanitize import (
    sanitize_table,
)

from doit.sanitizer.model import (
    TableSanitizer
)

def test_sanitize():
    unsanitizedtable_raw = dedent("""\
        (b),d,z,t
        1,2,a,b
        4,5,,c
        7,,d,e
    """)

    sanitizer_raw = dedent("""\
        (b),(d),c,a
        1,2,3,10
        4,5,,11
        7,,9,12
    """)

    expected_raw = dedent("""\
        c,a,d,z,t
        3,10,2,a,b
        ,11,5,,c
        9,12,,d,e
    """)

    sanitizer = TableSanitizer(
        table_name="test_table",
        sanitizers=(
            load_sanitizer_csv(sanitizer_raw, "test_sanitizer"),
        ),
    )

    unsanitizedtable = load_unsanitizedtable_csv(unsanitizedtable_raw, "CSV Import")

    sanitizedtable = sanitize_table(unsanitizedtable, sanitizer)

    expected_table = load_sanitizedtable_csv(expected_raw, "test_table")

    # Monkeypatch to put in the redacted value
    expected_table.data.rows[1]._map[SanitizedColumnId('c')] = Redacted() # type: ignore

    assert sanitizedtable.data == expected_table.data

