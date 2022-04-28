from __future__ import annotations
import typing as t
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    MetaData,
    Table,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import declarative_base

Base = declarative_base()

COLUMN_TYPE_LOOKUP = {
    'ordinal': Integer,
    'multiselect': String,
    'text': String,
}

def setup_datatable(metadata: MetaData, table: TableEntrySql) -> Table:
    return Table(
        table.name,
        metadata,
        *[
            Column(
                i.name,
                COLUMN_TYPE_LOOKUP[i.type],
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
        backref="parent_table",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries"
    id = Column(Integer, primary_key=True)
    parent_table_id = Column(Integer, ForeignKey(TableEntrySql.id), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    prompt = Column(String)
    sanitizer_checksum = Column(String)