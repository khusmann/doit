from __future__ import annotations
import typing as t
import enum
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    JSON,
    Table,
    Float,
    MetaData,
    Enum,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import declarative_base, backref

Base = declarative_base()

class CodemapSql(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    values = Column(JSON, nullable=False)

    column_entries: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        order_by="ColumnEntrySql.id"
    )

class MeasureEntrySql(Base):
    __tablename__ = "__measure_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntryType(enum.Enum):
    ORDINAL = 'ordinal'
    CATEGORICAL = 'categorical'
    TEXT = 'text'
    REAL = 'real'
    INTEGER = 'integer'
    GROUP = 'group'
    INDEX = 'index'

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries__"
    id = Column(Integer, primary_key=True)
    parent_measure_id = Column(Integer, ForeignKey(MeasureEntrySql.id))
    parent_column_id = Column(Integer, ForeignKey(id))
    codemap_id = Column(Integer, ForeignKey(CodemapSql.id))
    name = Column(String, nullable=False)
    shortname = Column(String)

    type = t.cast(Column[ColumnEntryType], Column(Enum(ColumnEntryType), nullable=False))

    prompt = Column(String)
    title = Column(String)
    description = Column(String)

    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnEntrySql.id",
    )

    instrument_nodes: RelationshipProperty[t.List[InstrumentEntrySql]] = relationship(
        "InstrumentNodeSql",
        order_by="InstrumentNodeSql.id"        
    )

    studytables: RelationshipProperty[t.List[StudyTableSql]] = relationship(
        "StudyTableSql",
        secondary=lambda: TableColumnAssociationSql,
        back_populates="columns",
    )

    parent_measure: RelationshipProperty[MeasureEntrySql | None] = relationship(
        "MeasureEntrySql",
        back_populates="items",
    )

    codemap: RelationshipProperty[CodemapSql | None] = relationship(
        "CodemapSql",
        back_populates="column_entries"
    )

def datatablecolumn_from_columnentrytype(type: ColumnEntryType):
    match type:
        case (
            ColumnEntryType.INDEX |
            ColumnEntryType.INTEGER |
            ColumnEntryType.CATEGORICAL |
            ColumnEntryType.ORDINAL
        ):
            return Integer
        case ColumnEntryType.TEXT:
            return String
        case ColumnEntryType.REAL:
            return Float
        case ColumnEntryType.GROUP:
            raise Exception("Error: cannot make a datatable column from a column group")

def setup_datatable(metadata: MetaData, table: StudyTableSql):
    return Table(
        table.name,
        metadata,
        *[
            Column(
                i.name,
                datatablecolumn_from_columnentrytype(i.type),
                primary_key=(i.type == ColumnEntryType.INDEX),
            ) for i in table.columns
        ]
    )

class StudyTableSql(Base):
    __tablename__ = "__table_info__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    columns: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        secondary=lambda: TableColumnAssociationSql,
        back_populates="studytables"
    )

    instruments: RelationshipProperty[t.List[InstrumentEntrySql]] = relationship(
        "InstrumentEntrySql",
        back_populates="studytable",
        order_by="InstrumentEntrySql.id",
    )

TableColumnAssociationSql = Table(
    "__table_column_association__",
    Base.metadata,
    Column('studytable_id', ForeignKey(StudyTableSql.id), primary_key=True),
    Column('column_entry_id', ForeignKey(ColumnEntrySql.id), primary_key=True),
)

class InstrumentEntrySql(Base):
    __tablename__ = "__instrument_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    title = Column(String)
    description = Column(String)
    instructions = Column(String)
    source_service = Column(String)
    source_title = Column(String)
    data_checksum = Column(String)
    schema_checksum = Column(String)

    studytable: RelationshipProperty[StudyTableSql | None] = relationship(
        "StudyTableSql",
    )

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        order_by="InstrumentNodeSql.id",
    )

class InstrumentNodeSql(Base):
    __tablename__ = "__instrument_nodes__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_instrument_id = Column(Integer, ForeignKey(InstrumentEntrySql.id))
    column_entry_id = Column(Integer, ForeignKey(ColumnEntrySql.id))
    source_column_name = Column(String)
    source_column_type = Column(String)
    source_prompt = Column(String)
    type = Column(String, nullable=False)

    source_value_map = Column(JSON)
    title = Column(String)
    prompt = Column(String)
    constant_value = Column(String)

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.id",
    )

    parent_instrument: RelationshipProperty[InstrumentEntrySql | None] = relationship(
        "InstrumentEntrySql",
        back_populates="items",
    )

    column_entry: RelationshipProperty[ColumnEntrySql | None] = relationship(
        "ColumnEntrySql",
        back_populates="instrument_nodes",
    )