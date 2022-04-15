# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    JSON,
    ForeignKey,
    Table,
    DateTime,
)
from sqlalchemy.ext.declarative import declarative_base

from ...domain.model import *

from sqlalchemy.orm import (
    backref,
    relationship,
)

Base = declarative_base()

class StudyTableSql(Base):
    __tablename__ = "__table_info__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    entity_type = 'studytable'
    columns = relationship(
        "ColumnInfoNodeSql",
        secondary=lambda: TableColumnAssociationSql,
    )

    def __repr__(self):
        return "Table(name: {}, columns={})".format(self.name, self.columns)

    def __init__(self, o: StudyTable):
        self.id=o.id
        self.name=o.name

class MeasureSql(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    entity_type = 'measure'
    items = relationship(
        "ColumnInfoNodeSql",
        backref="parent_measure",
        order_by="ColumnInfoNodeSql.id",
    )

    def __init__(self, o: Measure):
        self.id=o.id
        self.name=o.name
        self.title=o.title
        self.description=o.description

class CodeMapSql(Base):
    __tablename__ = "__codemaps__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    values = Column(JSON, nullable=False)
    entity_type = 'codemap'

    column_info = relationship(
        "ColumnInfoNodeSql",
        backref="content__codemap",
        order_by="ColumnInfoNodeSql.id"
    )

    def __init__(self, o: CodeMap):
        self.id=o.id
        self.name=o.name
        self.values=o.values

class DumpableNode:
    def __repr__(self):
        return "{}(type={}, id={})".format(
            type(self),
            self.type,
            self.id,
        )

    def dump(self, _indent=0):
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.items.values()])
        )

class ColumnInfoNodeSql(Base, DumpableNode):
    __tablename__ = "__column_info__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    root_measure_id = Column(Integer, ForeignKey(MeasureSql.id))
    content__codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))
    content__prompt = Column(String)
    content__title = Column(String)
    content__description = Column(String)
    content__type = Column(String, nullable=False)
    entity_type = 'column_info_node'
    content__items = relationship(
        "ColumnInfoNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnInfoNodeSql.id",
    )
    content__instrument_items = relationship(
        "InstrumentNodeSql",
        backref="content__column_info_node",
        order_by="InstrumentNodeSql.id"
    )

    def __init__(self, o: ColumnInfoNode):
        self.id=o.id
        self.parent_node_id=o.parent_node_id
        self.root_measure_id=o.root_measure_id
        self.name=o.name

        self.content__type=o.content.type
        match o.content:
            case MeasureItemGroup():
                self.content__prompt=o.content.prompt
            case OrdinalMeasureItem():
                self.content__prompt=o.content.prompt
                self.content__codemap_id=o.content.codemap_id
            case SimpleMeasureItem():
                self.content__prompt=o.content.prompt
            case IndexColumn():
                self.content__title=o.content.title
                self.content__description=o.content.description
                self.content__codemap_id=o.content.codemap_id


TableColumnAssociationSql = Table(
    "__table_column_association__",
    Base.metadata,
    Column('studytable_id', ForeignKey(StudyTableSql.id), primary_key=True),
    Column('column_info_node_id', ForeignKey(ColumnInfoNodeSql.id), primary_key=True),
)

class InstrumentSql(Base):
    __tablename__ = "__instruments__"
    id = Column(Integer, primary_key=True)
    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    instructions = Column(String)
    entity_type = 'instrument'
    source_table_info__remote_service = Column(String)
    source_table_info__remote_title = Column(String)
    source_table_info__last_fetched_utc = Column(DateTime)
    source_table_info__data_checksum = Column(String)
    source_table_info__schema_checksum = Column(String)

    items = relationship(
        "InstrumentNodeSql",
        backref="parent_instrument",
        order_by="InstrumentNodeSql.id",
    )

    def __init__(self, o: Instrument):
        self.id=o.id
        self.name=o.name
        self.studytable_id=o.studytable_id
        self.title=o.title
        self.description=o.description
        if o.source_table_info is not None:
            self.source_table_info__remote_service=o.source_table_info.remote_service
            self.source_table_info__remote_title=o.source_table_info.remote_title
            self.source_table_info__last_fetched_utc=o.source_table_info.last_fetched_utc
            self.source_table_info__data_checksum=o.source_table_info.data_checksum
            self.source_table_info__schema_checksum=o.source_table_info.schema_checksum        

class InstrumentNodeSql(Base, DumpableNode):
    __tablename__ = "__instrument_nodes__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    root_instrument_id = Column(Integer, ForeignKey(InstrumentSql.id))
    entity_type = 'instrument_node'
    content__column_info_node_id = Column(Integer, ForeignKey(ColumnInfoNodeSql.id))
    content__source_column_info__name = Column(String)
    content__source_column_info__type = Column(String)
    content__source_column_info__prompt = Column(String)
    content__type = Column(String, nullable=False)
    content__map = Column(JSON)
    content__title = Column(String)
    content__prompt = Column(String)
    content__value = Column(String)
    content__items = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.id",
    )

    def __init__(self, o: InstrumentNode):
        self.id=o.id
        self.parent_node_id=o.parent_node_id
        self.root_instrument_id=o.root_instrument_id
        self.content__type=o.content.type
        match o.content:
            case QuestionInstrumentItem():
                if o.content.source_column_info is not None:
                    self.content__source_column_info__name = o.content.source_column_info.name
                    self.content__source_column_info__type=o.content.source_column_info.type
                    self.content__source_column_info__prompt=o.content.source_column_info.prompt
                self.content__column_info_node_id=o.content.column_info_node_id
                self.content__prompt=o.content.prompt
                self.content__map=o.content.map
            case ConstantInstrumentItem():
                self.content__column_info_node_id=o.content.column_info_node_id
                self.content__value=o.content.value
            case InstrumentItemGroup():
                self.content__prompt=o.content.prompt
                self.content__title=o.content.title

SqlEntity = t.Union[
    CodeMapSql,
    MeasureSql,
    ColumnInfoNodeSql,
    InstrumentSql,
    InstrumentNodeSql,
    StudyTableSql,
]

sql_lookup: t.Mapping[t.Type[StudyEntity], SqlEntity] = {
    CodeMap: CodeMapSql,
    Measure: MeasureSql,
    ColumnInfoNode: ColumnInfoNodeSql,
    Instrument: InstrumentSql,
    InstrumentNode: InstrumentNodeSql,
    StudyTable: StudyTableSql,
}

sql_column_lookup: t.Mapping[StudyColumnTypeStr, t.Type[t.Any]] = {
    'ordinal': Integer,
    'text': String,
    'index': Integer,
    'categorical': Integer,
    'integer': Integer,
    'real': Float,
    'bool': Integer,
}