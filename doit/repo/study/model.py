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

from ...domain.value.study import *

from sqlalchemy.orm import (
    backref,
    relationship,
)

Base = declarative_base()

class StudyTableSql(Base):
    __tablename__ = "__tables__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    indices = Column(JSON, nullable=False)
    columns = relationship(
        "MeasureNodeSql",
        backref="parent_table"
    )

    def __repr__(self):
        return "Table(tag: {}, indices={})".format(self.tag, self.indices)

    def __init__(self, o: StudyTable):
        self.id=o.id
        self.name=o.name
        #TODO: self.indices=

class MeasureSql(Base):
    __tablename__ = "__measures__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    title = Column(String)
    description = Column(String)
    items = relationship(
        "MeasureNodeSql",
        backref="parent_measure",
        order_by="MeasureNodeSql.id",
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

class IndexColumnSql(Base):
    __tablename__ = "__indices__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))

    def __init__(self, o: IndexColumn):
        self.id=o.id
        self.name=o.name
        self.codemap_id=o.codemap_id

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

class MeasureNodeSql(Base, DumpableNode):
    __tablename__ = "__measure_nodes__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    parent_node_id = Column(Integer, ForeignKey(id))
    parent_measure_id = Column(Integer, ForeignKey(MeasureSql.id))
    studytable_id = Column(Integer, ForeignKey(StudyTableSql.id))
    codemap_id = Column(Integer, ForeignKey(CodeMapSql.id))
    prompt = Column(String)
    type = Column(String, nullable=False)
    items = relationship(
        "MeasureNodeSql",
        backref=backref("parent_node", remote_side=id),
        order_by="MeasureNodeSql.id",
    )

    def __init__(self, o: MeasureNode):
        self.id=o.id
        self.name=o.name
        self.parent_node_id=o.parent_node_id
        self.parent_measure_id=o.parent_measure_id
        self.prompt=o.prompt
        self.type=o.type
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
    parent_instrument_id = Column(Integer, ForeignKey(InstrumentSql.id))
    measure_node_id = Column(Integer, ForeignKey(MeasureNodeSql.id))
    index_column_id = Column(Integer, ForeignKey(IndexColumnSql.id))
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
        self.parent_instrument_id=o.parent_instrument_id
        self.type=o.type
        match o:
            case QuestionInstrumentItem():
                self.source_column_name=o.source_column_name
                self.measure_node_id=o.measure_node_id
                self.prompt=o.prompt
            case HiddenInstrumentItem():
                self.source_column_name=o.source_column_name
                self.measure_node_id=o.measure_node_id
            case ConstantInstrumentItem():
                self.measure_node_id=o.measure_node_id
                self.value=o.value
            case InstrumentItemGroup():
                self.prompt=o.prompt
                self.title=o.title