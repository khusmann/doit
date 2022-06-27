from __future__ import annotations
import typing as t

from sqlalchemy import (
    create_engine,
    MetaData,
)

from sqlalchemy.engine import Engine

from doit.study.view import ColumnRawView

from ...common.sqlalchemy import SessionWrapper

from ..spec import StudySpec
from ..repo import StudyRepoReader, StudyRepoWriter

from .sqlmodel import (
    Base,
    CodemapSql,
    ColumnEntrySql,
    ColumnEntryType,
    InstrumentEntrySql,
    MeasureEntrySql,
    setup_datatable,
    setup_measureview,
)

from ..model import LinkedTable

from .from_spec import (
    AddInstrumentContext,
    AddMeasureContext,
    sql_from_codemap_spec,
    sql_from_index_column_spec,
    render_tabledata,
)

from .to_view import (
    to_instrumentview,
    to_measureview,
    to_columnview,
    to_instrumentlinkerspec,
    to_instrumentlistingview,
    to_measurelistingview,
    to_columnrawview,
)

class SqlAlchemyRepo(StudyRepoWriter, StudyRepoReader):
    engine: Engine
    datatable_metadata: MetaData

    def __init__(self, engine: Engine, datatable_metadata: MetaData):
        self.engine = engine
        self.datatable_metadata = datatable_metadata

    @classmethod
    def create_engine(cls, filename: str):
        return create_engine("sqlite:///{}".format(filename))

    @classmethod
    def open(cls, filename: str = "") -> StudyRepoReader:
        engine = cls.create_engine(filename)
        datatables = cls.create_datatables(engine)
        return cls(engine, datatables)

    @classmethod
    def new(cls, spec: StudySpec, filename: str = "") -> StudyRepoWriter:
        engine = cls.create_engine(filename)
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

        session.commit()

        datatables = cls.create_datatables(engine)
       
        return cls(engine, datatables)

    @classmethod
    def create_datatables(cls, engine: Engine) -> MetaData:
        # Create instrument datatables
        session = SessionWrapper(engine)
        
        datatable_metadata = MetaData()

        instrument_tables = tuple(
            setup_datatable(datatable_metadata, entry)
                for entry in session.get_all(InstrumentEntrySql)
        )

        datatable_metadata.create_all(engine) 

        # Create measure views
        measure_views = tuple(
            setup_measureview(datatable_metadata, entry, instrument_tables)
                for entry in session.get_all(MeasureEntrySql)
        )

        for view in measure_views:
            if view is not None:
                engine.execute(view) # type: ignore

        # TODO: create package views

        return datatable_metadata
 
        
    def write_table(self, linked_table: LinkedTable):
        session = SessionWrapper(self.engine)

        sql_table = self.datatable_metadata.tables[linked_table.instrument_name]

        rows, errors = render_tabledata(linked_table)

        upsert_errors = session.upsert_rows(sql_table, rows) # TODO: this should just insert rows now, not upsert

        session.commit()

        return errors | upsert_errors

    def query_instrument(self, instrument_name: str):
        session = SessionWrapper(self.engine)
        return to_instrumentview(
            session.get_by_name(InstrumentEntrySql, instrument_name)
        )

    def query_instrumentlisting(self):
        session = SessionWrapper(self.engine)
        return to_instrumentlistingview(
            session.get_all(InstrumentEntrySql)
        )

    def query_measurelisting(self):
        session = SessionWrapper(self.engine)
        return to_measurelistingview(
            session.get_all(MeasureEntrySql)
        )

    def query_column_raw(self, patterns: t.Sequence[str]) -> t.Tuple[ColumnRawView, ...]:
        session = SessionWrapper(self.engine)
        all_entries = session.get_all(ColumnEntrySql)
        filtered_entries = {
            i
                for i in all_entries
                    for p in patterns
                        if i.name.startswith(p) and i.type != ColumnEntryType.GROUP
        }

        return tuple(to_columnrawview(e) for e in filtered_entries)


    def query_measure(self, measure_name: str):
        session = SessionWrapper(self.engine)
        return to_measureview(
            session.get_by_name(MeasureEntrySql, measure_name)
        )

    def query_studytable_by_instrument(self, instrument_name: str): # TODO return type Linker
        raise Exception("TODO: Not implemented")
#        session = SessionWrapper(self.engine)
#        instrument = session.get_by_name(InstrumentEntrySql, instrument_name)
#
#        if instrument.studytable is None:
#            raise Exception("Error: Instrument {} is not connected to a StudyTable".format(instrument_name))
#
#        return to_studytableview(
#            instrument.studytable
#        )

    def query_instrumentlinkerspecs(self):
        session = SessionWrapper(self.engine)
        entries = session.get_all(InstrumentEntrySql)
        return tuple(
            to_instrumentlinkerspec(i) for i in entries
        )
    
    def query_column(self, column_name: str):
        session = SessionWrapper(self.engine)
        return to_columnview(
            session.get_by_name(ColumnEntrySql, column_name)
        )