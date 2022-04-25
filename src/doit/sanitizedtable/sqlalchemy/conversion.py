from __future__ import annotations
import json

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
)

from ...common import TableValue

from .model import (
    Base,
    TableEntrySql,
    ColumnEntrySql,
)

from ...common import (
    Omitted,
    Some,
    ErrorValue,
    Redacted,
)

from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTableInfo,
)


COLUMN_TYPE_LOOKUP = {
    'ordinal': Integer,
    'multiselect': String,
    'text': String,
}

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
        data_checksum=str(entry.data_checksum),
        schema_checksum=str(entry.schema_checksum),
        columns=tuple(
            SanitizedColumnInfo(
                id=SanitizedColumnId(column.name),
                prompt=column.prompt,
                sanitizer_checksum=column.sanitizer_checksum,
                type=column.type,
            ) for column in entry.columns
        ),
    )

