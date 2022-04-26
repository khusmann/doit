from __future__ import annotations
import typing as t

from sqlalchemy import (
    create_engine,
    Table,
    insert,
)

from sqlalchemy.orm import (
    Session,
)

from sqlalchemy.engine import Engine

from ...common.sqlalchemy import (
    SessionWrapper
)

from .model import (
    Base,
    TableEntrySql,
)

from .conv import (
    render_tabledata,
    sql_from_tableinfo,
    sqlschema_from_tableinfo,
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
    datatables: t.Dict[str, Table]

    def __init__(self, engine: Engine, datatables: t.Mapping[str, Table]):
        self.engine = engine
        self.datatables = dict(datatables)

    @classmethod
    def create_engine(cls, filename: str):
        return create_engine("sqlite:///{}".format(filename))

    @classmethod
    def new(cls, filename: str = "") -> SanitizedTableRepoWriter:
        engine = cls.create_engine(filename)
        Base.metadata.create_all(engine)
        return cls(engine, {})

    @classmethod
    def open(cls, filename: str = "") -> SanitizedTableRepoReader:
        engine = cls.create_engine(filename)

        session = SessionWrapper(engine)

        datatables = {
            str(i.name): sqlschema_from_tableinfo(tableinfo_from_sql(i), str(i.name))
                for i in session.get_all(TableEntrySql)
        }

        return cls(engine, datatables)

    def write_table(self, table: SanitizedTable, name: str):
        self.datatables[name] = sqlschema_from_tableinfo(table.info, name)
        self.datatables[name].create(self.engine)

        session = Session(self.engine)

        session.add( # type: ignore
            sql_from_tableinfo(table.info, name)
        ) 

        session.execute( # type: ignore
            insert(self.datatables[name]).values(render_tabledata(table))
        )        

        session.commit()

    def read_tableinfo(self, name: str) -> SanitizedTableInfo:
        return tableinfo_from_sql(SessionWrapper(self.engine).get_by_name(TableEntrySql, name))

    def read_table(self, name: str) -> SanitizedTable:
        if name not in self.datatables:
            raise Exception("Error: No schema for {}".format(name))
        
        session = SessionWrapper(self.engine)
        
        info = tableinfo_from_sql(session.get_by_name(TableEntrySql, name))

        raw_rows = session.get_rows(self.datatables[name])

        data = tabledata_from_sql(info.columns, raw_rows) 

        return SanitizedTable(
            info=info,
            data=data,
        )
        

