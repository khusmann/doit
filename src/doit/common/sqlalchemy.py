import typing as t

from sqlalchemy import (
    select,
    insert,
    Table,
    MetaData
)

from sqlalchemy.orm import (
    Session,
    backref,
)

from sqlalchemy.engine import Engine

from sqlalchemy.ext.declarative import declarative_base

class DeclarativeBase:
    metadata: t.ClassVar[MetaData]
    def __init__(self, **kwargs: t.Any): ...

declarative_base: t.Callable[[], t.Type[DeclarativeBase]] = declarative_base
backref: t.Any = backref

T = t.TypeVar('T')

class SessionWrapper:
    impl: Session
    def __init__(self, engine: Engine):
        self.impl = Session(engine)

    def get_by_name(self, type: t.Type[T], name: str) -> T:
        entry: T | None = self.impl.execute( # type: ignore
            select(type).filter_by(name=name) # type: ignore
        ).scalars().one_or_none()

        if entry is None:
            raise Exception("Error: Entry named {} not found".format(name))

        return entry

    def get_all(self, type: t.Type[T]) -> t.Sequence[T]:
        return self.impl.execute( # type: ignore
            select(type) # type: ignore
        ).scalars().all()

    def get_rows(self, table: Table) -> t.List[t.Any]:
        return self.impl.execute( # type: ignore
            select(table.columns)
        ).all()

    def add(self, obj: t.Any):
        self.impl.add(obj) # type: ignore

    def commit(self):
        self.impl.commit()

    def insert_rows(self, table: Table, values: t.Sequence[t.Sequence[t.Any]]):
        self.impl.execute( # type: ignore
            insert(table).values(values)
        )        

