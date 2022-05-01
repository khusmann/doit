import typing as t

from sqlalchemy import (
    select,
    insert,
    update,
    Table,
    MetaData,
)

from sqlalchemy.orm import (
    Session,
    backref,
)

from sqlalchemy.engine import Engine, ResultProxy

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

    def get_or_create_by_name(self, type: t.Type[T], name: str) -> T:
        entry: T | None = self.impl.execute( # type: ignore
            select(type).filter_by(name=name) # type: ignore
        ).scalars().one_or_none()

        if entry is None:
            entry = type(name=name)

        return entry

    def get_all(self, type: t.Type[T]) -> t.Sequence[T]:
        return self.impl.execute( # type: ignore
            select(type) # type: ignore
        ).scalars().all()

    def get_rows(self, table: Table) -> ResultProxy:
        return self.impl.execute( # type: ignore
            select(table.columns)
        ).all()

    def add(self, obj: t.Any):
        self.impl.add(obj) # type: ignore

    def commit(self):
        self.impl.commit()

    def insert_rows(self, table: Table, values: t.Sequence[t.Mapping[str, t.Any]]):
        self.impl.execute( # type: ignore
            insert(table).values(values)
        )

    def upsert_rows(self, table: Table, rows: t.Sequence[t.Mapping[str, t.Any]]):
        for row in rows:
            index_filter = [k == row[k.name] for k in table.primary_key]
            
            exists: ResultProxy | None = self.impl.execute( # type:ignore
                select(table.columns).where(*index_filter)
            ).first()

            if exists:
                self.impl.execute( # type:ignore
                    update(table)
                        .where(*index_filter)
                        .values(row)
                )
            else:
                # TODO: test this... (insure we don't overwrite existing values...)
                non_null_value_filter = [ # type: ignore
                    c != None # type: ignore
                        for c in table.columns
                            if c.name in row and not c.primary_key
                ]

                has_values : ResultProxy | None = self.impl.execute( # type:ignore
                    select(table.columns).where(*index_filter, *non_null_value_filter)
                ).first() 

                if has_values:
                    raise Exception("Error: Values already exist in row. {}".format(has_values))

                self.impl.execute( # type: ignore
                    insert(table)
                        .values(row)
                )


