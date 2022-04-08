from __future__ import annotations
import typing as t
from pathlib import Path

from ..domain.value import *
from ..domain.model import *

from sqlite_utils import Database

SqlData = t.Mapping[SourceColumnName, t.Union[str, int, float, None]]

TABLE_INFO_NAME = "__table_info__"
COLUMN_INFO_NAME = "__column_info__"

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
        self.handle[table.instrument_name].insert_all(to_sql_table_data(table)) # type: ignore

        table_meta_dict = flatten_dict(table.info.dict(exclude={'columns'}))

        self.handle[TABLE_INFO_NAME].insert(table_meta_dict, pk="instrument_name") #type: ignore

        self.handle[COLUMN_INFO_NAME].insert_all( # type: ignore
            map(lambda i: { 'instrument_name': table.instrument_name, **i.dict() }, table.info.columns.values()),
            pk=("instrument_name", "source_column_name") # type: ignore
        )

class SourceTableRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.handle = Database(path)

    def query(self, instrument_name: InstrumentName) -> SourceTable:
        meta = self.query_meta(instrument_name)

        data_raw: t.Sequence[SqlData] = self.handle[instrument_name].rows # type: ignore

        return from_sql_table_data(meta, data_raw)

    def query_meta(self, instrument_name: InstrumentName) -> SourceTableInfo:
        column_meta_raw: t.Mapping[str, str] = self.handle[COLUMN_INFO_NAME].rows_where("instrument_name = ?", [instrument_name]) #type: ignore
        column_meta = { i.source_column_name: i for i in [SourceColumnInfo.parse_obj(i) for i in column_meta_raw] }

        table_meta_raw = unflatten_dict(self.handle[TABLE_INFO_NAME].get(instrument_name)) #type: ignore

        return SourceTableInfo.parse_obj({
            "columns": column_meta,
            **table_meta_raw,
        })

    def tables(self) -> t.List[InstrumentName]:
        table_names = set(self.handle.table_names()) - { TABLE_INFO_NAME, COLUMN_INFO_NAME }
        return [InstrumentName(i) for i in table_names]

def to_sql_table_data(table: SourceTable) -> t.Iterable[SqlData]:
    by_row = zip(*map(lambda i: i.values, table.data.values()))
    return map(lambda row: { key: value for (key, value) in zip(table.data.keys(), row) }, by_row)

def from_sql_column_data(column_info: SourceColumnInfo, data: t.List[t.Any]) -> SourceColumnData:
    match column_info.type:
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
    return SourceColumnData(
        source_column_name=column_info.source_column_name,
        type=column_info.type,
        values=values,
    )

def from_sql_table_data(info: SourceTableInfo, sql_data: t.Sequence[SqlData]) -> SourceTable:
    sql_data = tuple(sql_data)
    by_col = { column_name: [row[column_name] for row in sql_data] for column_name in info.columns.keys()}
    data_columns = {
        column_name: from_sql_column_data(column_info, by_col[column_name])
            for (column_name, column_info) in info.columns.items()
    }
    return SourceTable(
        instrument_name=info.instrument_name,
        info=info,
        data=data_columns,
    )