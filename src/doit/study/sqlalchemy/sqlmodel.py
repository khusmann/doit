from __future__ import annotations
import typing as t
import enum
from sqlalchemy import (
    Integer,
    String,
    ForeignKey,
    JSON,
    Table,
    Float,
    MetaData,
    Column,
)

from sqlalchemy.orm import (
    relationship,
    RelationshipProperty,
)

from ...common.sqlalchemy import (
    OptionalColumn,
    declarative_base,
    backref,
    RequiredColumn,
    RequiredEnumColumn,
)

Base = declarative_base()

class CodemapSql(Base):
    __tablename__ = "__codemaps__"
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)
    values = RequiredColumn(JSON)

    column_entries: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        order_by="ColumnEntrySql.id"
    )

class MeasureEntrySql(Base):
    __tablename__ = "__measure_entries__"
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)
    title = OptionalColumn(String)
    description = OptionalColumn(String)

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
    MULTISELECT = 'multiselect'

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries__"
    id = RequiredColumn(Integer, primary_key=True)
    parent_measure_id = OptionalColumn(Integer, ForeignKey(MeasureEntrySql.id))
    parent_column_id = OptionalColumn(Integer, ForeignKey(id))
    codemap_id = OptionalColumn(Integer, ForeignKey(CodemapSql.id))
    name = RequiredColumn(String)
    shortname = OptionalColumn(String)

    type = RequiredEnumColumn(ColumnEntryType)

    prompt = OptionalColumn(String)
    title = OptionalColumn(String)
    description = OptionalColumn(String)

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
        case ColumnEntryType.MULTISELECT:
            return JSON(none_as_null=True)
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
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)

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
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)
    studytable_id = OptionalColumn(Integer, ForeignKey(StudyTableSql.id))
    title = OptionalColumn(String)
    description = OptionalColumn(String)
    instructions = OptionalColumn(String)
    source_service = OptionalColumn(String)
    source_title = OptionalColumn(String)
    data_checksum = OptionalColumn(String)
    schema_checksum = OptionalColumn(String)

    studytable: RelationshipProperty[StudyTableSql | None] = relationship(
        "StudyTableSql",
    )

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        order_by="InstrumentNodeSql.sortkey",
    )

class InstrumentNodeType(enum.Enum):
    QUESTION = 'question'
    CONSTANT = 'constant'
    GROUP = 'group'

class InstrumentNodeSql(Base):
    __tablename__ = "__instrument_nodes__"
    id = RequiredColumn(Integer, primary_key=True)
    parent_node_id = OptionalColumn(Integer, ForeignKey(id))
    parent_instrument_id = OptionalColumn(Integer, ForeignKey(InstrumentEntrySql.id))
    column_entry_id = OptionalColumn(Integer, ForeignKey(ColumnEntrySql.id))
    source_column_name = OptionalColumn(String)
    source_column_type = OptionalColumn(String)
    source_prompt = OptionalColumn(String)
    type = RequiredEnumColumn(InstrumentNodeType)
    sortkey = RequiredColumn(Integer)

    source_value_map = OptionalColumn(JSON)
    title = OptionalColumn(String)
    prompt = OptionalColumn(String)
    constant_value = OptionalColumn(String)

    items: RelationshipProperty[t.List[InstrumentNodeSql]] = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.sortkey",
    )

    parent_instrument: RelationshipProperty[InstrumentEntrySql | None] = relationship(
        "InstrumentEntrySql",
        back_populates="items",
    )

    column_entry: RelationshipProperty[ColumnEntrySql | None] = relationship(
        "ColumnEntrySql",
        back_populates="instrument_nodes",
    )