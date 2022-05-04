from __future__ import annotations
import typing as t

from sqlalchemy.engine import ResultProxy

from ...common.table import (
    Omitted,
    Some,
    ErrorValue,
    Redacted,
    TableValue,
    TableRowView,
)

from .sqlmodel import (
    ColumnEntryType,
    TableEntrySql,
    ColumnEntrySql,
)


from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedCodedColumnInfo,
    SanitizedTableInfo,
    SanitizedTable,
    SanitizedTableData,
    SanitizedTextColumnInfo,
)

from pydantic import parse_obj_as

def tablevalue_from_sql(column: SanitizedColumnInfo, value: t.Any | None):
    if value is None:
        return Omitted()
    
    match column.value_type:
        case 'text':
            return Some(value).assert_type(str)
        case 'ordinal':
            return Some(value).assert_type(int) 
        case 'multiselect':
            return Some(value).assert_type_seq(int)

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

def render_value(column: SanitizedColumnInfo, value: TableValue[t.Any]):
    match column.value_type:
        case 'text':
            v = value.assert_type(str)
            match v:
                case Some(value=text):
                    return text
                case Redacted():
                    return "__REDACTED__"
                case Omitted():
                    return None
                case ErrorValue():
                    raise Exception("Error: Error value in text column {}".format(v))

        case 'ordinal':
            v = value.assert_type(int)
            match v:
                case Some(value=ord):
                    return ord
                case Omitted():
                    return None
                case Redacted():
                    raise Exception("Error: Redacted value in ordinal column {}".format(column.id.name))
                case ErrorValue():
                    raise Exception("Error: Error value in ordinal column {}".format(v))

        case 'multiselect':
            v = value.assert_type_seq(int)
            match v:
                case Some(value=multi):
                    return multi
                case Omitted():
                    return None
                case Redacted():
                    raise Exception("Error: Redacted value in multiselect column {}".format(column.id.name))
                case ErrorValue():
                    raise Exception("Error: Error value in multiselect column {}".format(v))


def sql_columnentrytype(info: SanitizedColumnInfo) -> ColumnEntryType:
    match info.value_type:
        case 'text':
            return ColumnEntryType.TEXT
        case 'multiselect':
            return ColumnEntryType.MULTISELECT
        case 'ordinal':
            return ColumnEntryType.ORDINAL

def sql_from_columninfo(info: SanitizedColumnInfo) -> ColumnEntrySql:
    match info:
        case SanitizedTextColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type=sql_columnentrytype(info),
                sanitizer_checksum=info.sanitizer_checksum,
            )
        case SanitizedCodedColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type=sql_columnentrytype(info),
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
    if not entry.prompt:
        raise Exception("Error: entry missing prompt {}".format(entry.name))

    match entry.type:
        case ColumnEntryType.TEXT:
            return SanitizedTextColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                sanitizer_checksum=entry.sanitizer_checksum,
                value_type=entry.type.value,
            )
        case ColumnEntryType.ORDINAL:
            return SanitizedCodedColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[int, str], entry.codes),
                value_type=entry.type.value,
            )
        case ColumnEntryType.MULTISELECT:
            return SanitizedCodedColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[int, str], entry.codes),
                value_type=entry.type.value,
            )