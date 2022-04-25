from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
)

from sqlalchemy.orm import (
    relationship,
    backref,
)

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MeasureEntrySql(Base):
    __tablename__ = "__measure_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items = relationship(
        "ColumnEntrySql",
        backref="parent_measure",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries__"
    id = Column(Integer, primary_key=True)
    parent_measure_id = Column(Integer, ForeignKey(MeasureEntrySql.id))
    parent_column_id = Column(Integer, ForeignKey(id))
    name = Column(String, nullable=False)

    type = Column(String, nullable=False)

    prompt = Column(String)
    title = Column(String)
    description = Column(String)

    items = relationship(
        "ColumnEntrySql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnEntrySql.id",
    )

