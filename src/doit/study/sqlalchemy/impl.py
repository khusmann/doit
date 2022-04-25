from __future__ import annotations
import typing as t

from sqlalchemy import (
    create_engine,
    Table,
    select,
)

from sqlalchemy.orm import (
    Session,
)

from sqlalchemy.engine import Engine

from ..spec import (
    StudySpec
)

from ..view import (
    StudyRepoReader,
    StudyRepoWriter,
    InstrumentView,
    MeasureView,
    ColumnView,
)

from .model import (
    Base,
    MeasureEntrySql,
)

from .from_spec import (
    sql_from_measure_spec
)

from .to_view import (
    to_instrumentview,
    to_measureview,
    to_columnview,
)

T = t.TypeVar('T')

class SessionWrapper(Session):
    def __init__(self, engine: Engine):
        super().__init__(engine)

    def get_by_name(self, type: t.Type[T], name: str) -> T:
        entry: T | None = self.execute( # type: ignore
            select(type).filter_by(name=name)
        ).scalars().one_or_none()

        if entry is None:
            raise Exception("Error: Entry named {} not found".format(name))

        return entry

    def get_all(self, type: t.Type[T]) -> t.Sequence[T]:
        return self.execute( # type: ignore
            select(type)
        ).scalars().all()

class SqlAlchemyRepo(StudyRepoWriter, StudyRepoReader):
    engine: Engine
    datatables: t.Dict[str, Table]

    def __init__(self, engine: Engine):
        self.engine = engine
        self.datatables = {}

    @classmethod
    def open(cls, filename: str = ""):
        return cls(
            create_engine("sqlite:///{}".format(filename)),
        )

    @classmethod
    def new(cls, spec: StudySpec, filename: str = ""):
        engine = create_engine("sqlite:///{}".format(filename))
        Base.metadata.create_all(engine)

        session = Session(engine)

        for name, measure in spec.measures.items():
            session.add( # type: ignore
                sql_from_measure_spec(measure, name)
            )

        session.commit()

        return SqlAlchemyRepo(engine)
        
    def write_table(self, table: str):
        pass

    def query_instrument(self, instrument_name: str) -> InstrumentView:
        return to_instrumentview()

    def query_measure(self, measure_name: str) -> MeasureView:
        return to_measureview(
            SessionWrapper(self.engine).get_by_name(MeasureEntrySql, measure_name)
        )

    def query_column(self, column_name: str) -> ColumnView:
        return to_columnview()