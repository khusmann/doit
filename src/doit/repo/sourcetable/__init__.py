from __future__ import annotations
import typing as t
from pathlib import Path
from sqlalchemy import create_engine, Table, insert
from sqlalchemy.orm import Session
from pydantic import parse_obj_as

from ...domain.value import *
from ...domain.model import *
from .model import *


def _create_datatable_def(table: SourceTableEntry):
    return Table(
        table.name,
        Base.metadata,
        Column("__id", Integer, primary_key=True), ## TODO: prevent collision with source column name here
        *[
            Column(
                i.content.name,
                sql_source_column_lookup[i.content.type],
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

        entry = self.query_info(instrument_name)

        datatable_sql = self.datatables[instrument_name]

        rowwise_data = session.query(datatable_sql).all()

        data = dict(zip([c.name for c in datatable_sql.columns.values()], (zip(*(row._mapping.values() for row in rowwise_data))))) # type: ignore

        return SourceTable(
            name=instrument_name,
            entry=entry,
            columns={
                name: SourceColumn(
                    name=name,
                    entry=column_entry,
                    values=data[name],
                ) for (name, column_entry) in entry.columns.items()
            }
        )


    def query_info(self, instrument_name: InstrumentName) -> SourceTableEntry:
        session = Session(self.engine)

        info_sql: t.Optional[SourceTableEntrySql] = (
            session.query(SourceTableEntrySql) # type: ignore
                   .filter(SourceTableEntrySql.name == instrument_name)
                   .one_or_none()
        )

        if info_sql is None:
            raise Exception("Table not found: {}".format(instrument_name))

        return SourceTableEntry.from_orm(info_sql)

    def query_info_all(self) -> t.Mapping[InstrumentName, SourceTableEntry]:
        session = Session(self.engine)
        all_info_sql: t.List[SourceTableEntrySql] = session.query(SourceTableEntrySql).all()
        return { i.name: i for i in parse_obj_as(t.List[SourceTableEntry], all_info_sql) }

    def tables(self) -> t.List[InstrumentName]:
        return list(self.query_info_all().keys())


def rowwise(m: t.Mapping[SourceColumnName, SourceColumn]):
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

        sql_table_info = SourceTableEntrySql(table.entry)

        sql_column_info = (SourceColumnEntrySql(i) for i in table.entry.columns.values())

        session.add(sql_table_info) # type: ignore
        for i in sql_column_info:
            session.add(i) # type: ignore

        self.datatables[table.entry.name] = _create_datatable_def(table.entry)
        self.datatables[table.entry.name].create(self.engine)

        rowwise_data = list(rowwise(table.columns))

        session.execute( # type: ignore
            insert(self.datatables[table.entry.name])
                .values(rowwise_data)
        )

        session.commit()