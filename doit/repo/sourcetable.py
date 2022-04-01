from __future__ import annotations
import typing as t
from pathlib import Path

from ..domain.value import (
    SourceColumnName,
    InstrumentName,
    SourceColumn,
    SourceTable,
    SourceTableMeta,
    SourceColumnMeta,
    new_source_column
)

from sqlite_utils import Database

SqlData = t.Mapping[SourceColumnName, t.Union[str, int, float, None]]

TABLE_META_NAME = "__table_meta__"
COLUMN_META_NAME = "__column_meta__"

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

def unflatten_dict(dictionary: t.Mapping[str, str]) -> t.Mapping[str, t.Any]:
    resultDict: t.Mapping[str, t.Any] = dict()
    for key, value in dictionary.items():
        parts = key.split(".")
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict

class SourceTableRepoWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        self.handle = Database(path)

    def insert(self, table: SourceTable) -> None:
        self.handle[table.instrument_id].insert_all(to_sql_table_data(table)) # type: ignore

        table_meta_dict = flatten_dict(table.meta.dict(exclude={'columns'}))

        self.handle[TABLE_META_NAME].insert(table_meta_dict, pk="instrument_id") #type: ignore

        self.handle[COLUMN_META_NAME].insert_all( # type: ignore
            map(lambda i: { 'instrument_id': table.instrument_id, **i.dict() }, table.meta.columns.values()),
            pk=("instrument_id", "column_id") # type: ignore
        )

class SourceTableRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.handle = Database(path)

    def query(self, instrument_id: InstrumentName) -> SourceTable:
        meta = self.query_meta(instrument_id)

        data_raw: t.Sequence[SqlData] = self.handle[instrument_id].rows # type: ignore

        return from_sql_table_data(meta, data_raw)

    def query_meta(self, instrument_id: InstrumentName) -> SourceTableMeta:
        column_meta_raw: t.Mapping[str, str] = self.handle[COLUMN_META_NAME].rows_where("instrument_id = ?", [instrument_id]) #type: ignore
        column_meta = { i.column_id: i for i in [SourceColumnMeta.parse_obj(i) for i in column_meta_raw] }

        table_meta_raw = unflatten_dict(self.handle[TABLE_META_NAME].get(instrument_id)) #type: ignore

        return SourceTableMeta.parse_obj({
            "columns": column_meta,
            **table_meta_raw,
        })

    def tables(self) -> t.List[InstrumentName]:
        table_names = set(self.handle.table_names()) - { TABLE_META_NAME, COLUMN_META_NAME }
        return [InstrumentName(i) for i in table_names]

def to_sql_table_data(table: SourceTable) -> t.Iterable[SqlData]:
    by_row = zip(*map(lambda i: i.values, table.columns.values()))
    return map(lambda row: { key: value for (key, value) in zip(table.columns.keys(), row) }, by_row)

def from_sql_column_data(meta: SourceColumnMeta, data: t.List[t.Any]) -> SourceColumn:
    match meta.type:
        case 'bool':
            values=[None if i is None else bool(i) for i in data]
        case 'integer':
            values=data
        case 'text':
            values=data
        case 'ordinal':
            values=data
        case 'real':
            values=data
    return new_source_column(
        column_id=meta.column_id,
        meta=meta,
        column_type=meta.type,
        values=values,
    )

def from_sql_table_data(meta: SourceTableMeta, sql_data: t.Sequence[SqlData]) -> SourceTable:
    sql_data = tuple(sql_data)
    by_col = { column_id: [row[column_id] for row in sql_data] for column_id in meta.columns.keys()}
    columns = {
        column_id: from_sql_column_data(column_meta, by_col[column_id]) for (column_id, column_meta) in meta.columns.items()
    }
    return SourceTable(
        instrument_id=meta.instrument_id,
        meta=meta,
        columns=columns,
    )