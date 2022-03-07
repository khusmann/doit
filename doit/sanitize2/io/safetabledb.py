from __future__ import annotations
import typing as t
from pathlib import Path

from pydantic import BaseModel

from ..domain.value import ColumnId, InstrumentId, SafeTable, SafeTableMeta
from sqlite_utils import Database

SqlData = t.Mapping[ColumnId, t.Union[str, int, float, None]]

class SqlTable(BaseModel):
    data: t.Iterable[SqlData]

def flatten_dict(y: t.Mapping[str, t.Any]) -> t.Mapping[str, t.Any]:
    out: t.Mapping[str, t.Any] = {}

    def flatten(x: t.Mapping[str, t.Any], name: str=''):
        match x:
            case dict():
                for a in x:
                    flatten(x[a], name + a + '.')
            case _:
                out[name[:-1]] = x


    flatten(y)
    return out

class SafeTableDbWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        self.handle = Database(path)

    def insert(self, table: SafeTable) -> None:
        self.handle[table.instrument_id].insert_all(to_sql_table_data(table)) # type: ignore

        table_meta_dict = flatten_dict(table.meta.dict(exclude={'columns'}))

        self.handle["__table_meta__"].insert(table_meta_dict, pk="instrument_id") #type: ignore

        self.handle["__column_meta__"].insert_all( # type: ignore
            map(lambda i: { 'instrument_id': table.instrument_id, **i.dict() }, table.meta.columns.values()),
            pk=("instrument_id", "column_id") # type: ignore
        )

class SafeTableDbReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.handle = Database(path)

    def query(self, instrument_id: InstrumentId) -> SafeTable:
        return SafeTable(
            title="asdf",
            columns={}
        )

    def query_meta(self, instrument_id: InstrumentId) -> SafeTableMeta:
        return SafeTableMeta(
            title="asdf",
            columns={}
        )

    def tables(self) -> t.List[InstrumentId]:
        return []

def to_sql_table_data(table: SafeTable) -> t.Iterable[SqlData]:
    by_row = zip(*map(lambda i: i.values, table.columns.values()))
    return map(lambda row: { key: value for (key, value) in zip(table.columns.keys(), row) }, by_row)