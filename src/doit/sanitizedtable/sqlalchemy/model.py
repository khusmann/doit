from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
)

from sqlalchemy.orm import (
    relationship,
)

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TableEntrySql(Base):
    __tablename__ = "__table_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    data_checksum = Column(String, nullable=False)
    schema_checksum = Column(String, nullable=False)
    columns = relationship(
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