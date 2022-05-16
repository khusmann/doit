from __future__ import annotations
import typing as t
import enum
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    MetaData,
    Table,
    JSON,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import (
    declarative_base,
    RequiredColumn,
    OptionalColumn,
    RequiredEnumColumn,
)

Base = declarative_base()

def datatablecolumn_from_columnentrytype(type: ColumnEntryType):
    match type:
        case ColumnEntryType.ORDINAL:
            return Integer
        case ColumnEntryType.MULTISELECT:
            return JSON(none_as_null=True)
        case ColumnEntryType.TEXT:
            return String

def setup_datatable(metadata: MetaData, table: TableEntrySql) -> Table:
    return Table(
        table.name,
        metadata,
        *[
            Column(
                i.name,
                datatablecolumn_from_columnentrytype(i.type),
            ) for i in table.columns
        ]
    )

class TableEntrySql(Base):
    __tablename__ = "__table_entries__"
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)
    title = RequiredColumn(String)
    source = RequiredColumn(String)
    data_checksum = RequiredColumn(String)
    schema_checksum = RequiredColumn(String)

    columns: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntryType(enum.Enum):
    ORDINAL = 'ordinal'
    MULTISELECT = 'multiselect'
    TEXT = 'text'

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries"
    id = RequiredColumn(Integer, primary_key=True)
    parent_table_id = RequiredColumn(Integer, ForeignKey(TableEntrySql.id))
    name = RequiredColumn(String)
    type = RequiredEnumColumn(ColumnEntryType)
    sortkey = RequiredColumn(String)

    prompt = OptionalColumn(String)
    sanitizer_checksum = OptionalColumn(String)
    codes = OptionalColumn(JSON)