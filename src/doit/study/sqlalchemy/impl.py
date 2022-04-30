from __future__ import annotations
import typing as t

from sqlalchemy import (
    create_engine,
    MetaData,
    select,
    insert,
    update,
)

from sqlalchemy.engine import Engine

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
    StudyTableSql,
    setup_datatable,
)

from ..model import LinkedTable

from .from_spec import (
    AddInstrumentContext,
    AddMeasureContext,
    sql_from_codemap_spec,
    sql_from_index_column_spec,
    sql_from_tablevalue,
)

from .to_view import (
    to_instrumentview,
    to_measureview,
    to_columnview,
    to_studytableview,
    to_instrumentlinkerspec,
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
        session = SessionWrapper(engine)

        datatable_metadata = MetaData()
        for entry in session.get_all(StudyTableSql):
            setup_datatable(datatable_metadata, entry)

        return cls(engine, datatable_metadata)

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

        # Add Studytables
        for instrument in session.get_all(InstrumentEntrySql):
            all_columns: t.List[ColumnEntrySql] = [
                i.column_entry
                    for i in instrument.items
                        if i.column_entry is not None
            ]

            index_columns = [i for i in all_columns if i.type == ColumnEntryType.INDEX]

            if not index_columns:
                raise Exception("Error: instrument {} has no indices".format(instrument.name))

            table_name = "-".join(sorted(i.shortname for i in index_columns))

            table = session.get_or_create_by_name(StudyTableSql, table_name)

            table.columns.extend(i for i in all_columns if i not in table.columns)
            table.instruments.append(instrument)

        # Verify each column belongs to only one Studytable (TODO: Test this)
        for column in session.get_all(ColumnEntrySql):
            if len(column.studytables) > 1 and column.type != ColumnEntryType.INDEX:
                raise Exception("Error: column {} found in muliple tables. Check the indices in the associated instruments".format(column.name))

        datatable_metadata = MetaData()
        for entry in session.get_all(StudyTableSql):
            table = setup_datatable(datatable_metadata, entry)

        session.commit()

        for table in datatable_metadata.tables.values():
            table.create(engine)
        
        return cls(engine, datatable_metadata)
        
    def write_table(self, table: LinkedTable):

        session = SessionWrapper(self.engine)
        curr_table = self.datatable_metadata.tables[table.studytable_name]

        for row in table.data.rows:
            # TODO: fix _map
            sql_row = { id.linked_name: sql_from_tablevalue(tv) for id, tv in row._map.items() } # type: ignore
            index_params = [k == sql_row[k.name] for k in curr_table.primary_key]
            
            exists = session.impl.execute( # type:ignore
                select(curr_table.columns).where(*index_params)
            ).one_or_none()

            if exists:
                session.impl.execute( # type:ignore
                    update(curr_table)
                        .where(*index_params)
                        .values(sql_row)
                )
            else:
                session.impl.execute( # type: ignore
                    insert(curr_table)
                        .values(sql_row)
                )

        session.commit()

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

        if instrument.studytable is None:
            raise Exception("Error: Instrument {} is not connected to a StudyTable".format(instrument_name))

        return to_studytableview(
            instrument.studytable
        )

    def query_instrumentlinkerspecs(self):
        session = SessionWrapper(self.engine)
        entries = session.get_all(InstrumentEntrySql)
        return tuple(
            to_instrumentlinkerspec(i) for i in entries
                if i.studytable is not None
        )
    
    def query_column(self, column_name: str):
        session = SessionWrapper(self.engine)
        return to_columnview(
            session.get_by_name(ColumnEntrySql, column_name)
        )