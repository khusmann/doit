from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine, Table, insert
from sqlalchemy.orm import Session
from pydantic import parse_obj_as

from ...domain.value import *
from ...domain.model import *
from .model import *


def _create_datatable_def(table: SourceTableInfo):
    return Table(
        table.name,
        Base.metadata,
        Column("__id", Integer, primary_key=True), ## TODO: prevent collision with source column name here
        *[
            Column(
                i.name,
                sql_source_column_lookup[i.type],
            ) for i in table.columns.values()
        ]
    )

class SourceTableRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.path = path
        self.engine = create_engine("sqlite:///{}".format(path), echo=False)

        self.datatables: t.Dict[InstrumentName, Table] = {
            name: _create_datatable_def(table_info) for name, table_info in self.query_info_all().items()
        }

    def query(self, instrument_name: InstrumentName) -> SourceTable:
        session = Session(self.engine)

        info = self.query_info(instrument_name)

        datatable_sql = self.datatables[instrument_name]

        rowwise_data = session.query(datatable_sql).all()

        data = dict(zip([c.name for c in datatable_sql.columns.values()], (zip(*(row._mapping.values() for row in rowwise_data))))) # type: ignore

        return SourceTable(
            name=instrument_name,
            info=info,
            data={
                name: SourceColumnData(
                    name=name,
                    type=column_info.type,
                    values=data[name],
                ) for (name, column_info) in info.columns.items()
            }
        )


    def query_info(self, instrument_name: InstrumentName) -> SourceTableInfo:
        session = Session(self.engine)

        info_sql: t.Optional[SourceTableInfoSql] = (
            session.query(SourceTableInfoSql) # type: ignore
                   .filter(SourceTableInfoSql.name == instrument_name)
                   .one_or_none()
        )

        if info_sql is None:
            raise Exception("Table not found: {}".format(instrument_name))

        return SourceTableInfo.from_orm(info_sql)

    def query_info_all(self) -> t.Mapping[InstrumentName, SourceTableInfo]:
        session = Session(self.engine)
        all_info_sql: t.List[SourceTableInfo] = session.query(SourceTableInfoSql).all()
        return { i.name: i for i in parse_obj_as(t.List[SourceTableInfo], all_info_sql) }

    def tables(self) -> t.List[InstrumentName]:
        return list(self.query_info_all().keys())


def rowwise(m: t.Mapping[SourceColumnName, SourceColumnData]):
    return (dict(zip(m.keys(), v)) for v in zip(*(i.values for i in m.values())))

class SourceTableRepo(SourceTableRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.path = path
        self.engine = create_engine("sqlite:///{}".format(path), echo=False)
        Base.metadata.create_all(self.engine)
        self.datatables: t.Dict[InstrumentName, Table] = {}


    def add_source_table(self, table: SourceTable) -> None:
        session = Session(self.engine)

        sql_table_info = SourceTableInfoSql(table.info)

        sql_column_info = (SourceColumnInfoSql(i) for i in table.info.columns.values())

        session.add(sql_table_info) # type: ignore
        for i in sql_column_info:
            session.add(i) # type: ignore

        self.datatables[table.name] = _create_datatable_def(table.info)
        self.datatables[table.name].create(self.engine)

        rowwise_data = list(rowwise(table.data))

        session.execute( # type: ignore
            insert(self.datatables[table.name])
                .values(rowwise_data)
        )

        session.commit()