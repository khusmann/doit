from __future__ import annotations
import typing as t

from sqlalchemy import create_engine, Table
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from ...common.sqlalchemy import SessionWrapper


from ..spec import StudySpec
from ..repo import StudyRepoReader, StudyRepoWriter

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

class SqlAlchemyRepo(StudyRepoWriter, StudyRepoReader):
    engine: Engine
    datatables: t.Dict[str, Table]

    def __init__(self, engine: Engine):
        self.engine = engine
        self.datatables = {}

    @classmethod
    def open(cls, filename: str = "") -> StudyRepoReader:
        return cls(
            create_engine("sqlite:///{}".format(filename)),
        )

    @classmethod
    def new(cls, spec: StudySpec, filename: str = "") -> StudyRepoWriter:
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

    def query_instrument(self, instrument_name: str):
        return to_instrumentview()

    def query_measure(self, measure_name: str):
        return to_measureview(
            SessionWrapper(self.engine).get_by_name(MeasureEntrySql, measure_name)
        )

    def query_linker(self, instrument_name: str): # TODO return type Linker
        pass
    
    def query_column(self, column_name: str):
        return to_columnview()