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
    StudyTableSql,
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
    to_studytableview,
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

        # Add codemaps
        for measure_name, measure in spec.measures.items():
            for codemap_name, codemap in measure.codes.items():
                session.add(sql_from_codemap_spec(codemap, measure_name, codemap_name))

        # Add Indices
        for index_name, index in spec.config.indices.items():
            session.add(sql_from_index_column_spec(index, index_name))

        # Add Measures
        for measure_name, measure in spec.measures.items():
            session.add(
                AddMeasureContext(
                    get_codemap_by_relname=lambda codemap_relname: session.get_by_name(CodemapSql, ".".join((measure_name, codemap_relname)))
                ).sql_from_measure_spec(
                    measure,
                    measure_name
                )
            )

        # Add Instruments    
        for instrument_name, instrument in spec.instruments.items():
            session.add(
                AddInstrumentContext(
                    get_column_by_name=lambda column_name: session.get_by_name(ColumnEntrySql, column_name),
                ).sql_from_instrument_spec(
                    instrument,
                    instrument_name,
                )
            )

        # Add Studytables
        for instrument in session.get_all(InstrumentEntrySql):
            all_columns: t.List[ColumnEntrySql] = [
                i.column_entry
                    for i in instrument.items
                        if i.column_entry is not None
            ]

            index_columns = [i for i in all_columns if i.type == 'index']

            if not index_columns:
                raise Exception("Error: instrument {} has no indices".format(instrument.name))

            table_name = "-".join(sorted(i.shortname for i in index_columns))

            table = session.get_or_create_by_name(StudyTableSql, table_name)

            table.columns.extend(all_columns)
            table.instruments.append(instrument)

        # Verify each column belongs to only one Studytable (TODO: Test this)
        for column in session.get_all(ColumnEntrySql):
            if len(column.studytables) > 1 and column.type != 'index':
                raise Exception("Error: column {} found in muliple tables. Check the indices in the associated instruments".format(column.name))

        session.commit()

        return SqlAlchemyRepo(engine)
        
    def write_table(self, table: str):
        pass

    def query_instrument(self, instrument_name: str):
        session = SessionWrapper(self.engine)
        return to_instrumentview(
            session.get_by_name(InstrumentEntrySql, instrument_name)
        )

    def query_measure(self, measure_name: str):
        session = SessionWrapper(self.engine)
        return to_measureview(
            session.get_by_name(MeasureEntrySql, measure_name)
        )

    def query_studytable_by_instrument(self, instrument_name: str): # TODO return type Linker
        session = SessionWrapper(self.engine)
        instrument = session.get_by_name(InstrumentEntrySql, instrument_name)
        return to_studytableview(
            instrument.studytable
        )
    
    def query_column(self, column_name: str):
        session = SessionWrapper(self.engine)
        return to_columnview(
            session.get_by_name(ColumnEntrySql, column_name)
        )