from __future__ import annotations
import typing as t
from collections import abc

from sqlalchemy.engine import ResultProxy

from ...common.table import (
    Omitted,
    Some,
    Multi,
    ErrorValue,
    Redacted,
    TableValue,
    TableRowView,
)

from .sqlmodel import (
    TableEntrySql,
    ColumnEntrySql,
)


from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedMultiselectColumnInfo,
    SanitizedOrdinalColumnInfo,
    SanitizedTableInfo,
    SanitizedTable,
    SanitizedTableData,
    SanitizedTextColumnInfo,
)

from pydantic import parse_obj_as

def tablevalue_from_sql(column: SanitizedColumnInfo, value: t.Any):
    if value is None:
        return Omitted()

    if isinstance(column, SanitizedMultiselectColumnInfo):
        if isinstance(value, str) or not isinstance(value, abc.Sequence):
            raise Exception("Error: multiselect values should be sequences. Type found: {} value: {}".format(type(value), value))
        value = t.cast(t.Sequence[t.Any], value)
        return Multi(tuple(int(i) for i in value))

    return Some(value)

def tabledata_from_sql(columns: t.Sequence[SanitizedColumnInfo], rows: ResultProxy):
    return SanitizedTableData(
            column_ids=tuple(c.id for c in columns),
            rows=tuple(
                TableRowView(
                    (c.id, tablevalue_from_sql(c, row[c.id.name])) for c in columns
                ) for row in rows
            )
        )

def render_tabledata(table: SanitizedTable):
    return [
        { c.id.name: render_value(c, row.get(c.id)) for c in table.info.columns }
            for row in table.data.rows
    ]

def render_value(column: SanitizedColumnInfo, v: TableValue):
    if isinstance(v, ErrorValue):
        print("Encountered error value: {}".format(v))
        return None

    match column:
        case SanitizedTextColumnInfo():
            match v:
                case Some(value=value):
                    return str(value)
                case Redacted():
                    return "__REDACTED__"
                case Omitted():
                    return None
                case _:
                    raise Exception("Error: Unexpected value in text column {}".format(v))

        case SanitizedOrdinalColumnInfo():
            match v:
                case Some(value=value):
                    return int(value)
                case Omitted():
                    return None
                case _:
                    raise Exception("Error: Unexpected value in ordinal column {}".format(v))

        case SanitizedMultiselectColumnInfo():
            match v:
                case Multi(values=values):
                    return values
                case Omitted():
                    return None
                case _:
                    raise Exception("Error: Unexpected value in multiselect column {}".format(v))

def sql_from_columninfo(info: SanitizedColumnInfo) -> ColumnEntrySql:
    match info:
        case SanitizedTextColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type="text",
                sanitizer_checksum=info.sanitizer_checksum,
            )
        case SanitizedOrdinalColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type="ordinal",
                codes=info.codes,
            )
        case SanitizedMultiselectColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type="multiselect",
                codes=info.codes,
            )


def sql_from_tableinfo(info: SanitizedTableInfo, name: str) -> TableEntrySql:
    return TableEntrySql(
        name=name,
        data_checksum=info.data_checksum,
        schema_checksum=info.schema_checksum,
        columns=[ sql_from_columninfo(column) for column in info.columns ],
    )

def tableinfo_from_sql(entry: TableEntrySql) -> SanitizedTableInfo:
    return SanitizedTableInfo(
        data_checksum=entry.data_checksum,
        schema_checksum=entry.schema_checksum,
        columns=tuple(
            columninfo_from_sql(column) for column in entry.columns
        ),
    )

def columninfo_from_sql(entry: ColumnEntrySql) -> SanitizedColumnInfo:
    match entry.type:
        case "text":
            return SanitizedTextColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                sanitizer_checksum=entry.sanitizer_checksum,
            )
        case "ordinal":
            return SanitizedOrdinalColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[int, str], entry.codes),
            )
        case "multiselect":
            return SanitizedMultiselectColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[int, str], entry.codes),
            )
        case _:
            raise Exception("Error: TODO: Add enum for column type")

