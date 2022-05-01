from __future__ import annotations
import typing as t
import json

from sqlalchemy.engine import ResultProxy

from ...common.table import (
    Omitted,
    Some,
    ErrorValue,
    Redacted,
    TableValue,
    TableRowView,
    OrdinalLabel,
    OrdinalValue,
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

def tabledata_from_sql(column_names: t.Sequence[SanitizedColumnInfo], rows: ResultProxy):
    return SanitizedTableData(
            column_ids=tuple(c.id for c in column_names),
            rows=tuple(
                TableRowView({
                    c.id: Some(row[c.id.name]) if row[c.id.name] else Omitted() for c in column_names
                }) for row in rows
            )
        )

def render_tabledata(table: SanitizedTable):
    return [
        { c.id.name: render_value(c, row.get(c.id)) for c in table.info.columns }
            for row in table.data.rows
    ]

def render_value(column: SanitizedColumnInfo, v: TableValue):
    if isinstance(v, Omitted):
        return None

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
        case SanitizedOrdinalColumnInfo():
            match v:
                case Some(value=value):
                    return int(value)
                case Redacted():
                    raise Exception("Error: Unexpected redacted value in a non-text column")
        case SanitizedMultiselectColumnInfo():
            match v:
                case Some(value=value):
                    return json.dumps(v.value)
                case Redacted():
                    raise Exception("Error: Unexpected redacted value in a non-text column")

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
                codes=parse_obj_as(t.Mapping[OrdinalValue, OrdinalLabel], entry.codes),
            )
        case "multiselect":
            return SanitizedMultiselectColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[OrdinalValue, OrdinalLabel], entry.codes),
            )
        case _:
            raise Exception("Error: TODO: Add enum for column type")

