from __future__ import annotations
import typing as t

from sqlalchemy import create_engine, Table
from sqlalchemy.engine import Engine

from ...common.sqlalchemy import SessionWrapper

from ..spec import StudySpec
from ..repo import StudyRepoReader, StudyRepoWriter

from .sqlmodel import (
    Base,
    CodemapSql,
    ColumnEntrySql,
    InstrumentEntrySql,
    MeasureEntrySql,
)

from .from_spec import (
    AddInstrumentContext,
    AddMeasureContext,
    sql_from_codemap_spec,
    sql_from_index_column_spec,
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

        session = SessionWrapper(engine)

        for measure_name, measure in spec.measures.items():
            for codemap_name, codemap in measure.codes.items():
                session.add(sql_from_codemap_spec(codemap, measure_name, codemap_name))

        for index_name, index in spec.config.indices.items():
            session.add(sql_from_index_column_spec(index, index_name))

        for measure_name, measure in spec.measures.items():
            session.add(
                AddMeasureContext(
                    lambda codemap_relname: session.get_by_name(CodemapSql, ".".join((measure_name, codemap_relname)))
                ).sql_from_measure_spec(
                    measure,
                    measure_name
                )
            )

        for instrument_name, instrument in spec.instruments.items():
            session.add(
                AddInstrumentContext(
                    lambda column_name: session.get_by_name(ColumnEntrySql, column_name)
                ).sql_from_instrument_spec(
                    instrument,
                    instrument_name,
                )
            )

        session.commit()

        return SqlAlchemyRepo(engine)
        
    def write_table(self, table: str):
        pass

    def query_instrument(self, instrument_name: str):
        return to_instrumentview(
            SessionWrapper(self.engine).get_by_name(InstrumentEntrySql, instrument_name)
        )

    def query_measure(self, measure_name: str):
        return to_measureview(
            SessionWrapper(self.engine).get_by_name(MeasureEntrySql, measure_name)
        )

    def query_linker(self, instrument_name: str): # TODO return type Linker
        pass
    
    def query_column(self, column_name: str):
        return to_columnview(
            SessionWrapper(self.engine).get_by_name(ColumnEntrySql, column_name)
        )