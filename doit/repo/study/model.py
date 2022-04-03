# type: ignore
from __future__ import annotations
import typing as t

from sqlalchemy import (
    Column,
    Integer,
    String,
    JSON,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

from ...domain.model import *

from sqlalchemy.orm import (
    backref,
    relationship,
)

Base = declarative_base()

class StudyTableSql(Base):
    __tablename__ = "__tables__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    index_names = Column(JSON, nullable=False)
    columns = relationship(
        "ColumnInfoNodeSql",
        backref="parent_table"
    )

    def __repr__(self):
        return "Table(tag: {}, indices={})".format(self.tag, self.indices)

    def __init__(self, o: StudyTable):
        self.id=o.id
        self.name=o.name
        self.index_names=o.index_names

class MeasureSql(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
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

    def __init__(self, o: CodeMap):
        self.id=o.id
        self.name=o.name
        self.values=o.values

class DumpableNode:
    def __repr__(self):
        return "{}(type={}, tag={})".format(
            type(self),
            self.type,
            self.tag,
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
    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))
    prompt = Column(String)
    title = Column(String)
    description = Column(String)
    type = Column(String, nullable=False)
    items = relationship(
        "ColumnInfoNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="ColumnInfoNodeSql.id",
    )

    def __init__(self, o: MeasureNode | IndexColumn):
        self.id=o.id
        self.name=o.name
        self.type=o.type
        if (o.type == 'index'):
            self.title=o.title
            self.description=o.description
            self.codemap_id=o.codemap_id
        else:
            self.parent_node_id=o.parent_node_id
            self.root_measure_id=o.root_measure_id
            self.prompt=o.prompt
            match o:
                case MeasureItemGroup():
                    pass
                case OrdinalMeasureItem():
                    self.studytable_id=o.studytable_id
                    self.codemap_id=o.codemap_id
                case SimpleMeasureItem():
                    self.studytable_id=o.studytable_id

class InstrumentSql(Base):
    __tablename__ = "__instruments__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    instructions = Column(String)
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

class InstrumentNodeSql(Base, DumpableNode):
    __tablename__ = "__instrument_nodes__"
    id = Column(Integer, primary_key=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    root_instrument_id = Column(Integer, ForeignKey(InstrumentSql.id))
    column_info_id = Column(Integer, ForeignKey(ColumnInfoNodeSql.id))
    source_column_name = Column(String)
    type = Column(String, nullable=False)
    title = Column(String)
    prompt = Column(String)
    value = Column(String)
    items = relationship(
        "InstrumentNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="InstrumentNodeSql.id",
    )

    def __init__(self, o: InstrumentNode):
        self.id=o.id
        self.parent_node_id=o.parent_node_id
        self.root_instrument_id=o.root_instrument_id
        self.type=o.type
        match o:
            case QuestionInstrumentItem():
                self.source_column_name=o.source_column_name
                self.column_info_id=o.column_info_id
                self.prompt=o.prompt
            case HiddenInstrumentItem():
                self.source_column_name=o.source_column_name
                self.column_info_id=o.column_info_id
            case ConstantInstrumentItem():
                self.column_info_id=o.column_info_id
                self.value=o.value
            case InstrumentItemGroup():
                self.prompt=o.prompt
                self.title=o.title

sql_lookup: t.Mapping[t.Type[StudyEntity], Base] = {
    CodeMap: CodeMapSql,
    Measure: MeasureSql,
    OrdinalMeasureItem: ColumnInfoNodeSql,
    SimpleMeasureItem: ColumnInfoNodeSql,
    MeasureItemGroup: ColumnInfoNodeSql,
    IndexColumn: ColumnInfoNodeSql,
    Instrument: InstrumentSql,
    QuestionInstrumentItem: InstrumentNodeSql,
    ConstantInstrumentItem: InstrumentNodeSql,
    HiddenInstrumentItem: InstrumentNodeSql,
    InstrumentItemGroup: InstrumentNodeSql,
    StudyTable: StudyTableSql,
}