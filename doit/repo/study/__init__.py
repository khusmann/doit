from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine, select

from sqlalchemy.orm import Session, contains_eager

from ...domain.value import *
from ...domain.service import *
from ...domain.model import *

from .model import *

from pydantic import parse_obj_as

EntityT = t.TypeVar('EntityT', bound=StudyEntity)
NamedEntityT = t.TypeVar('NamedEntityT', bound=NamedStudyEntity)

class StudyRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=False)

    def query_instruments(self) -> t.List[Instrument]:
        session = Session(self.engine)
        result = (
            session.query(InstrumentSql) # type: ignore
                   .options(contains_eager(InstrumentSql.items))
                   .join(InstrumentNodeSql)
                   .filter(InstrumentNodeSql.parent_node_id == None)
                   .all()
        )
        return parse_obj_as(t.List[Instrument], result)

    def query_measures(self) -> t.List[Measure]:
        session = Session(self.engine)
        result = (
            session.query(MeasureSql) # type: ignore
                   .options(contains_eager(MeasureSql.items))
                   .join(ColumnInfoNodeSql)
                   .filter(ColumnInfoNodeSql.parent_node_id == None)
                   .all()
        )
        return parse_obj_as(t.List[Measure], result)

    def query_instrument(self, name: InstrumentName) -> Instrument:
        session = Session(self.engine)
        result: t.Optional[InstrumentSql] = (
            session.query(InstrumentSql) # type: ignore
                   .options(contains_eager(InstrumentSql.items))
                   .join(InstrumentNodeSql)
                   .filter(InstrumentSql.name == name)
                   .filter(InstrumentNodeSql.parent_node_id == None)
                   .one_or_none()
        )
        if result is None:
            raise Exception("Instrument not found (name={})".format(name))
        return parse_obj_as(Instrument, result)

    def query_measure(self, name: MeasureName) -> Measure:
        session = Session(self.engine)
        result: t.Optional[MeasureSql] = (
            session.query(MeasureSql) # type: ignore
                   .options(contains_eager(MeasureSql.items))
                   .join(ColumnInfoNodeSql)
                   .filter(MeasureSql.name == name)
                   .filter(ColumnInfoNodeSql.parent_node_id == None)
                   .one_or_none()
        )
        if result is None:
            raise Exception("Measure not found (name={})".format(name))
        return parse_obj_as(Measure, result)

    def query_entity_by_id(
        self,
        id: StudyEntityId,
        entity_type: t.Type[EntityT],
        type_str: t.Optional[str] = None,
    ) -> EntityT:

        session = Session(self.engine)
        sql_type = sql_lookup[entity_type]

        result = session.execute( # type: ignore
            select(sql_type).where(sql_type.id == id)
        ).scalar_one_or_none()

        if result is None:
            raise Exception("{} not found (id={})".format(type_str or entity_type.__name__, id))

        return parse_obj_as(EntityT, result) # type: ignore

    def query_entity_by_name(
        self,
        name: StudyEntityName,
        entity_type: t.Type[NamedStudyEntity],
        type_str: t.Optional[str] = None,
    ) -> NamedStudyEntity:

        session = Session(self.engine)
        sql_type = sql_lookup[entity_type]

        assert(not isinstance(sql_type, InstrumentNodeSql)) # InstrumentNodes don't have a unique name

        result = session.execute( # type: ignore
            select(sql_type).where(sql_type.name == name)
        ).scalar_one_or_none()

        if result is None:
            raise Exception("{} not found (name={})".format(type_str or entity_type.__name__, name))

        return parse_obj_as(EntityT, result) # type: ignore

    def query_column_info_by_name(self, name: ColumnName) -> ColumnInfo:
        return self.query_entity_by_name(name, ColumnInfo, "ColumnInfo") # type: ignore

class StudyRepo(StudyRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=False)
        Base.metadata.create_all(self.engine)

    def add_entities(self, entities: t.Sequence[StudyEntity]):
        session = Session(self.engine)

        for e in entities:
            session.add(sql_lookup[type(e)](e)) # type: ignore

        session.commit()

        return self