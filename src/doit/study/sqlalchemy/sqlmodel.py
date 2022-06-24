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
    select,
    union_all,
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

from sqlalchemy_views import CreateView # type: ignore

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
    indices = RequiredColumn(JSON)

    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        order_by="ColumnEntrySql.sortkey",
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
    sortkey = RequiredColumn(Integer)

    type = RequiredEnumColumn(ColumnEntryType)

    prompt = OptionalColumn(String)
    title = OptionalColumn(String)
    description = OptionalColumn(String)

    items: RelationshipProperty[t.List[ColumnEntrySql]] = relationship(
        "ColumnEntrySql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnEntrySql.sortkey",
    )

    instrument_nodes: RelationshipProperty[t.List[InstrumentEntrySql]] = relationship(
        "InstrumentNodeSql",
        order_by="InstrumentNodeSql.id"        
    )

    parent_measure: RelationshipProperty[MeasureEntrySql | None] = relationship(
        "MeasureEntrySql",
        back_populates="items",
    )

    codemap: RelationshipProperty[CodemapSql | None] = relationship(
        "CodemapSql",
        back_populates="column_entries"
    )

def datatablecolumn_from_columnentrytype(type: ColumnEntryType, name: str):
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
            raise Exception("Error: cannot make a datatable column from a column group {}".format(name))

def setup_datatable(metadata: MetaData, table: InstrumentEntrySql):
    columns = tuple((
        *(i.column_entry for i in table.items if i.column_entry and i.column_entry.type == ColumnEntryType.INDEX),
        *(i.column_entry for i in table.items if i.column_entry and i.column_entry.type != ColumnEntryType.INDEX),
    ))
    return Table(
        table.name,
        metadata,
        *[
            Column(
                i.name,
                datatablecolumn_from_columnentrytype(i.type, i.name),
                primary_key=(i.type == ColumnEntryType.INDEX),
            ) for i in columns
        ]
    )

def makequery(measure: MeasureEntrySql, i: Table):
    from sqlalchemy.sql.expression import null

    if all(m.name not in i.c for m in measure.items):
        return None

    for idx in measure.indices:
        if idx not in i.c:
            raise Exception("Index {} not found in {}".format(idx, i.name))


    indices = tuple(i.c[j] for j in measure.indices)
    datacols = tuple(i.c[m.name] if m.name in i.c else null().label(m.name) for m in measure.items if m.type != ColumnEntryType.GROUP)

    return select((*indices, *datacols))

def setup_measureview(metadata: MetaData, measure: MeasureEntrySql, instruments: t.Sequence[Table]):
    queries = tuple(
        makequery(measure, i) for i in instruments    
    )

    filtered_queries = tuple(q for q in queries if q is not None)

    if filtered_queries:
        view = Table(measure.name, metadata)
        return CreateView(view, union_all(*filtered_queries))
    else:
        return None

class InstrumentEntrySql(Base):
    __tablename__ = "__instrument_entries__"
    id = RequiredColumn(Integer, primary_key=True)
    name = RequiredColumn(String, unique=True)
    title = OptionalColumn(String)
    description = OptionalColumn(String)
    instructions = OptionalColumn(String)
    source_service = OptionalColumn(String)
    source_title = OptionalColumn(String)
    data_checksum = OptionalColumn(String)
    schema_checksum = OptionalColumn(String)
    exclude_filters = RequiredColumn(JSON)

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