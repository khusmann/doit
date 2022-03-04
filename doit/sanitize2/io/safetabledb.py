from __future__ import annotations
import typing as t
from pathlib import Path

from pydantic import BaseModel
from ..domain.value import ColumnId, InstrumentId, SafeTable
from sqlite_utils import Database

SqlData = t.Mapping[ColumnId, t.Union[str, int, float, None]]

class SqlTable(BaseModel):
    data: t.Iterable[SqlData]

class SafeTableDbWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        self.handle = Database(path)

    def insert(self, instrument_id: InstrumentId, table: SafeTable) -> None:
        self.handle[instrument_id].insert_all(to_sql_data(table)) # type: ignore

class SafeTableDbReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.handle = Database(path)

    def query(self, instrument_id: InstrumentId) -> SafeTable:
        return SafeTable(
            title="asdf",
            columns={}
        )

    def query_meta(self, instrument_id: InstrumentId) -> str:
        return "meta"

    def tables(self) -> t.List[InstrumentId]:
        return []

def to_sql_data(table: SafeTable) -> t.Iterable[SqlData]:
    by_row = zip(*map(lambda i: i.values, table.columns.values()))
    return map(lambda row: { key: value for (key, value) in zip(table.columns.keys(), row) }, by_row)