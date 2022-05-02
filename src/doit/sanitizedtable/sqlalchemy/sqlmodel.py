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
    Enum,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import declarative_base

Base = declarative_base()

def datatablecolumn_from_columnentrytype(type: ColumnEntryType):
    match type:
        case ColumnEntryType.ORDINAL:
            return Integer
        case ColumnEntryType.MULTISELECT:
            return JSON
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
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    data_checksum = Column(String, nullable=False)
    schema_checksum = Column(String, nullable=False)
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
    id = Column(Integer, primary_key=True)
    parent_table_id = Column(Integer, ForeignKey(TableEntrySql.id), nullable=False)
    name = Column(String, nullable=False)
    type = t.cast(Column[ColumnEntryType], Column(Enum(ColumnEntryType), nullable=False))
    prompt = Column(String)
    sanitizer_checksum = Column(String)
    codes = Column(JSON)