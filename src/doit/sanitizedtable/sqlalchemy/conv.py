from __future__ import annotations
import typing as t
import json

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
)

from ...common.table import (
    Omitted,
    Some,
    ErrorValue,
    Redacted,
    TableValue,
    TableRowView,
)

from .sqlmodel import (
    Base,
    TableEntrySql,
    ColumnEntrySql,
)


from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTableInfo,
    SanitizedTable,
    SanitizedTableData,
)

COLUMN_TYPE_LOOKUP = {
    'ordinal': Integer,
    'multiselect': String,
    'text': String,
}

def tabledata_from_sql(columns: t.Sequence[SanitizedColumnInfo], rows: t.Sequence[t.Any]):
    return SanitizedTableData(
            column_ids=tuple(c.id for c in columns),
            rows=tuple(
                TableRowView({
                    c.id: Some(v) if v else Omitted() for c, v in zip(columns, row)
                }) for row in rows
            )
        )

def render_tabledata(table: SanitizedTable):
    return [
        tuple(render_value(c, row.get(c.id)) for c in table.info.columns)
            for row in table.data.rows
    ]

def render_value(column: SanitizedColumnInfo, v: TableValue):
    if isinstance(v, Omitted):
        return None

    if isinstance(v, ErrorValue):
        print("Encountered error value: {}".format(v))
        return None

    if column.type == 'text':
        match v:
            case Some(value=value):
                return str(value)
            case Redacted():
                return "__REDACTED__"

    if isinstance(v, Redacted):
        print("Unexpected redacted value in a non-text column")
        return None

    match column.type:
        case 'ordinal':
            return int(v.value)
        case 'multiselect':
            return json.dumps(v.value)

def sqlschema_from_tableinfo(table: SanitizedTableInfo, name: str) -> Table:
    return Table(
        name,
        Base.metadata,
        *[
            Column(
                i.id.name,
                COLUMN_TYPE_LOOKUP[i.type],
            ) for i in table.columns
        ]
    )

def sql_from_tableinfo(info: SanitizedTableInfo, name: str) -> TableEntrySql:
    return TableEntrySql(
        name=name,
        data_checksum=info.data_checksum,
        schema_checksum=info.schema_checksum,
        columns=[
            ColumnEntrySql(
                name=column.id.name,
                type=column.type,
                prompt=column.prompt,
            ) for column in info.columns
        ]
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
    return SanitizedColumnInfo(
        id=SanitizedColumnId(entry.name),
        prompt=entry.prompt,
        sanitizer_checksum=entry.sanitizer_checksum,
        type=entry.type, # type: ignore TODO: handle different SanitizedColumnInfo types
    )

