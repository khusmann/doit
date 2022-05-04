from textwrap import dedent

from doit.sanitizedtable.io import (
    load_sanitizedtable_csv,
)

from doit.sanitizedtable.sqlalchemy.impl import (
    SqlAlchemyRepo,
)

def test_invariance():
    sanitizedtable_raw = dedent("""\
        c,a,d,z,t
        3,10,2,a,b
        ,11,5,,c
        9,12,,d,e
    """)

    sanitizedtable = load_sanitizedtable_csv(sanitizedtable_raw, "test_table")

    repo = SqlAlchemyRepo.new()

    assert isinstance(repo, SqlAlchemyRepo)

    repo.write_table(sanitizedtable)

    result = repo.read_table("test_table")

    assert result == sanitizedtable


