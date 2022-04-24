from textwrap import dedent

from doit.sanitizedtable.impl.csv import (
    load_sanitizedtable_csv,
)

from doit.sanitizedtable.impl.sqlalchemy import (
    SqlAlchemyRepo,
)

def test_invariance():
    sanitizedtable_raw = dedent("""\
        c,a,d,z,t
        3,10,2,a,b
        ,11,5,,c
        9,12,,d,e
    """)

    sanitizedtable = load_sanitizedtable_csv(sanitizedtable_raw)

    repo = SqlAlchemyRepo()

    repo.write_table(sanitizedtable, "test-table")

    result = repo.read_table("test-table")

    assert result == sanitizedtable


