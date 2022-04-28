from __future__ import annotations

from sqlalchemy import (
    create_engine,
    MetaData,
)

from sqlalchemy.engine import Engine
from ...common.sqlalchemy import SessionWrapper

from .sqlmodel import (
    Base,
    TableEntrySql,
    setup_datatable,
)

from .conv import (
    render_tabledata,
    sql_from_tableinfo,
    tabledata_from_sql,
    tableinfo_from_sql,
)

from ..model import (
    SanitizedTable,
    SanitizedTableInfo,
)

from ..repo import (
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

class SqlAlchemyRepo(SanitizedTableRepoReader, SanitizedTableRepoWriter):
    engine: Engine
    datatable_metadata: MetaData

    def __init__(self, engine: Engine, datatable_metadata: MetaData):
        self.engine = engine
        self.datatable_metadata = datatable_metadata

    @classmethod
    def create_engine(cls, filename: str):
        return create_engine("sqlite:///{}".format(filename))

    @classmethod
    def new(cls, filename: str = "") -> SanitizedTableRepoWriter:
        engine = cls.create_engine(filename)
        Base.metadata.create_all(engine)
        return cls(engine, MetaData())

    @classmethod
    def open(cls, filename: str = "") -> SanitizedTableRepoReader:
        engine = cls.create_engine(filename)

        session = SessionWrapper(engine)

        datatable_metadata = MetaData()
        for i in session.get_all(TableEntrySql):
            setup_datatable(datatable_metadata, i)

        return cls(engine, datatable_metadata)

    def write_table(self, table: SanitizedTable, name: str):
        session = SessionWrapper(self.engine)

        entry = sql_from_tableinfo(table.info, name)
        session.add(entry) 

        new_table = setup_datatable(self.datatable_metadata, entry)
        new_table.create(self.engine)

        session.insert_rows(new_table, render_tabledata(table))

        session.commit()

    def read_tableinfo(self, name: str) -> SanitizedTableInfo:
        session = SessionWrapper(self.engine)
        return tableinfo_from_sql(session.get_by_name(TableEntrySql, name))

    def read_table(self, name: str) -> SanitizedTable:
        if name not in self.datatable_metadata.tables:
            raise Exception("Error: No schema for {}".format(name))
        
        session = SessionWrapper(self.engine)
        
        info = tableinfo_from_sql(session.get_by_name(TableEntrySql, name))

        raw_rows = session.get_rows(self.datatable_metadata.tables[name])

        data = tabledata_from_sql(info.columns, raw_rows) 

        return SanitizedTable(
            info=info,
            data=data,
        )
        

