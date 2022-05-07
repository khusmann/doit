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
    TableErrorReport,
    TableErrorReportItem,
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
    SanitizedSimpleColumnInfo,
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
    errors: TableErrorReport = set()

    def filter_error(column: SanitizedColumnInfo, value: TableValue[t.Any]):
        name = column.id.name
        filtered_value = render_value(column, value)
        if isinstance(filtered_value, ErrorValue):
            errors.add(TableErrorReportItem(table.info.name, name, filtered_value))
            return None
        else:
            return filtered_value

    return (
        tuple(
            { c.id.name: filter_error(c, row.get(c.id)) for c in table.info.columns }
                for row in table.data.rows
        ),
        errors,
    )

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
        case SanitizedSimpleColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type=sql_columnentrytype(info),
                sanitizer_checksum=info.sanitizer_checksum,
                sortkey=info.sortkey,
            )
        case SanitizedCodedColumnInfo():
            return ColumnEntrySql(
                name=info.id.name,
                prompt=info.prompt,
                type=sql_columnentrytype(info),
                codes=info.codes,
                sortkey=info.sortkey,
            )

def sql_from_tableinfo(info: SanitizedTableInfo) -> TableEntrySql:
    return TableEntrySql(
        name=info.name,
        title=info.title,
        data_checksum=info.data_checksum,
        schema_checksum=info.schema_checksum,
        columns=[ sql_from_columninfo(column) for column in info.columns ],
    )

def tableinfo_from_sql(entry: TableEntrySql) -> SanitizedTableInfo:
    return SanitizedTableInfo(
        name=entry.name,
        title=entry.title,
        data_checksum=entry.data_checksum,
        schema_checksum=entry.schema_checksum,
        columns=tuple(
            sorted(
                (columninfo_from_sql(column) for column in entry.columns),
                key=lambda x: x.sortkey,
            )
        ),
    )

def columninfo_from_sql(entry: ColumnEntrySql) -> SanitizedColumnInfo:
    if not entry.prompt:
        raise Exception("Error: entry missing prompt {}".format(entry.name))

    match entry.type:
        case ColumnEntryType.TEXT:
            return SanitizedSimpleColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                sanitizer_checksum=entry.sanitizer_checksum,
                value_type=entry.type.value,
                sortkey=entry.sortkey,
            )
        case ColumnEntryType.ORDINAL | ColumnEntryType.MULTISELECT:
            return SanitizedCodedColumnInfo(
                id=SanitizedColumnId(entry.name),
                prompt=entry.prompt,
                codes=parse_obj_as(t.Mapping[int, str], entry.codes),
                value_type=entry.type.value,
                sortkey=entry.sortkey,
            )