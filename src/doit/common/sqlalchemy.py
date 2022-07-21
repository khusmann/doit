import typing as t
import enum

from sqlalchemy import (
    select,
    insert,
    update,
    Table,
    MetaData,
    Column,
    Enum,
    or_,
)

from sqlalchemy.sql.type_api import TypeEngine

from sqlalchemy.orm import (
    Session,
    backref,
)

from sqlalchemy.engine import Engine, ResultProxy

from sqlalchemy.ext.declarative import declarative_base

from .table import (
    TableErrorReportItem,
    ValuesAlreadyExistInRow,
    ErrorValue,
)

class DeclarativeBase:
    metadata: t.ClassVar[MetaData]
    def __init__(self, **kwargs: t.Any): ...

declarative_base: t.Callable[[], t.Type[DeclarativeBase]] = declarative_base
backref: t.Any = backref

T = t.TypeVar('T')

EnumT = t.TypeVar('EnumT', bound=enum.Enum)

def RequiredColumn(type: t.Type[TypeEngine[T]], constraint: t.Any | None = None, primary_key: bool = False, unique: bool = False) -> Column[T]:
    return Column(type, constraint, nullable=False, primary_key=primary_key, unique=unique)

def RequiredEnumColumn(type: t.Type[EnumT], constraint: t.Any | None = None, primary_key: bool = False, unique: bool = False) -> Column[EnumT]:
    return t.cast(Column[EnumT], Column(Enum(type), constraint, nullable=False, primary_key=primary_key, unique=unique))

def OptionalColumn(type: t.Type[TypeEngine[T]], constraint: t.Any | None = None, unique: bool = False) -> Column[T | None]:
    return Column(type, constraint, nullable=True, unique=unique)

def OptionalEnumColumn(type: t.Type[EnumT], constraint: t.Any | None = None, primary_key: bool = False, unique: bool = False) -> Column[t.Optional[EnumT]]:
    return t.cast(Column[t.Optional[EnumT]], Column(Enum(type), constraint, nullable=True, primary_key=primary_key, unique=unique))

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
        upsert_errors: set[TableErrorReportItem] = set()

        for row in rows:
            index_filter = [k == row[k.name] for k in table.primary_key]
            
            exists: ResultProxy | None = self.impl.execute( # type:ignore
                select(table.columns).where(*index_filter)
            ).first()

            if exists:
                # TODO: make a unit test for this... (insure we don't overwrite existing values...)
                non_null_value_filter = [ # type: ignore
                    c != None # type: ignore
                        for c in table.columns
                            if c.name in row and not c.primary_key
                ]

                has_values : ResultProxy | None = self.impl.execute( # type:ignore
                    select(table.columns).where(*index_filter, or_(*non_null_value_filter)) # type: ignore
                ).first() 

                if has_values:
                    upsert_errors.add(
                        TableErrorReportItem(
                            table_name=table.name,
                            column_name="*",
                            error_value=ErrorValue(ValuesAlreadyExistInRow(has_values, row)),
                        )
                    )

                self.impl.execute( # type:ignore
                    update(table)
                        .where(*index_filter)
                        .values(row)
                )
            else:
                self.impl.execute( # type: ignore
                    insert(table)
                        .values(row)
                )

        return upsert_errors


