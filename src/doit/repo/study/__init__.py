from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine, select, insert, update

from sqlalchemy.orm import Session, contains_eager

from ...domain.value import *
from ...domain.model import *

from .model import *

from pydantic import parse_obj_as

EntityT = t.TypeVar('EntityT', bound=StudyEntity)
NamedEntityT = t.TypeVar('NamedEntityT', bound=NamedStudyEntity)

RootEntityT = t.TypeVar(
    'RootEntityT',
    t.Tuple[t.Type[InstrumentSql], t.Type[InstrumentNodeSql]],
    t.Tuple[t.Type[MeasureSql], t.Type[ColumnInfoNodeSql]],
)

class StudyRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.path = path
        self.engine = create_engine("sqlite:///{}".format(path), echo=False)
        self.datatables_cache: t.Optional[t.Mapping[StudyTableId, Table]] = None

    def _query_entity_by_id(
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

    def _query_all_entities(
        self,
        entity_type: t.Type[EntityT],
    ) -> t.Tuple[EntityT, ...]:

        session = Session(self.engine)
        sql_type = sql_lookup[entity_type]

        result = session.query(sql_type).all()

        return parse_obj_as(t.Tuple[EntityT, ...], result)

    def _query_entity_by_name(
        self,
        name: StudyEntityName,
        entity_type: t.Type[NamedEntityT],
        type_str: t.Optional[str] = None,
    ) -> NamedEntityT:

        session = Session(self.engine)
        sql_type = sql_lookup[entity_type]

        assert(not isinstance(sql_type, InstrumentNodeSql)) # InstrumentNodes don't have a unique name

        result = session.execute( # type: ignore
            select(sql_type).where(sql_type.name == name)
        ).scalar_one_or_none()

        if result is None:
            raise Exception("{} not found (name={})".format(type_str or entity_type.__name__, name))

        return parse_obj_as(NamedEntityT, result) # type: ignore

    def _query_roots(self, parent_child: RootEntityT, conv_type: t.Type[EntityT]) -> t.Tuple[EntityT, ...]:
        session = Session(self.engine)
        parent, child = parent_child
        result = (
            session.query(parent) # type: ignore
                   .options(contains_eager(parent.items))
                   .join(child)
                   .filter(child.parent_node_id == None)
                   .order_by(parent.title)
                   .all()
        )
        return parse_obj_as(t.Tuple[EntityT, ...], result)

    def _query_root(self, name: str, parent_child: RootEntityT, conv_type: t.Type[EntityT]) -> EntityT:
        session = Session(self.engine)
        parent, child = parent_child
        result: t.Optional[parent] = (
            session.query(parent) # type: ignore
                   .options(contains_eager(parent.items))
                   .join(child)
                   .filter(parent.name == name)
                   .filter(child.parent_node_id == None)
                   .one_or_none()
        )

        if result is None:
            raise Exception("Measure not found (name={})".format(name))

        return parse_obj_as(EntityT, result) # type: ignore

    def query_measures(self) -> t.Tuple[Measure, ...]:
        return self._query_roots((MeasureSql, ColumnInfoNodeSql), Measure)

    def query_instruments(self) -> t.Tuple[Instrument, ...]:
        return self._query_roots((InstrumentSql, InstrumentNodeSql), Instrument)

    def query_measure(self, name: MeasureName) -> Measure:
        return self._query_root(name, (MeasureSql, ColumnInfoNodeSql), Measure)

    def query_instrument(self, name: InstrumentName) -> Instrument:
        return self._query_root(name, (InstrumentSql, InstrumentNodeSql), Instrument)

    def query_column_info(self, name: ColumnName) -> ColumnInfoNode:
        return self._query_entity_by_name(name, ColumnInfoNode)

    def query_studytable(self, name: StudyTableName) -> StudyTable:
        return self._query_entity_by_name(name, StudyTable)

    def query_studytables(self) -> t.Tuple[StudyTable, ...]:
        return self._query_all_entities(StudyTable)

    @property
    def datatables(self) -> t.Mapping[StudyTableId, Table]:
        # TODO: there's some state dragons in here.
        # Mutations that change the schema should not be allowed after this is called.
        if not self.datatables_cache:
            self.datatables_cache = self._create_datatable_defs()
        return self.datatables_cache

    def _create_datatable_defs(self):
        # TODO: Validate studytable info, that is, verify that each measure belongs to no more than ONE studytable
        study_table_infos = self.query_studytables()

        return {
            i.id: Table(
                i.name,
                Base.metadata,
                *[Column(c.name, sql_column_lookup[c.content.type], primary_key=True) for c in i.columns if c.content.type == 'index'],
                *[Column(c.name, sql_column_lookup[c.content.type]) for c in i.columns if c.content.type != 'index' and not isinstance(c.content, MeasureItemGroup)],
            ) for i in study_table_infos if i.columns is not None
        }

class StudyRepo(StudyRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.path = path
        self.engine = create_engine("sqlite:///{}".format(path), echo=False)
        self.datatables_cache: t.Optional[t.Mapping[StudyTableId, Table]] = None
        Base.metadata.create_all(self.engine)

    def create_tables(self):
        self.datatables_cache = self._create_datatable_defs()
        Base.metadata.create_all(self.engine)
        return StudyRepoDataWriter(self)

    def mutate(self, mutations: t.Sequence[AddEntityMutation]):
        assert(self.datatables_cache is None)
        
        session = Session(self.engine)

        for m in mutations:
            match m:
                case AddSimpleEntityMutation():
                    session.add(sql_lookup[type(m.entity)](m.entity)) # type: ignore
                case AddStudyTableMutation():
                    session.add(sql_lookup[type(m.table)](m.table)) # type: ignore
                    session.execute( # type: ignore
                        insert(TableColumnAssociationSql), [
                            { 'studytable_id': m.table.id, 'column_info_node_id': column_info_node_id }
                            for column_info_node_id in m.column_info_node_ids
                        ]
                    )

        session.commit()

        return self

def rowwise(m: t.Mapping[ColumnName, t.Iterable[t.Any]]):
    return (dict(zip(m.keys(), v)) for v in zip(*m.values()))

class StudyRepoDataWriter(StudyRepoReader):
    def __init__(self, repo: StudyRepo):
        self.path = repo.path
        self.engine = repo.engine
        self.datatables_cache = repo.datatables_cache

    def add_source_data(self, mutation: AddSourceDataMutation):
        session = Session(self.engine)

        rowwise_data = rowwise(mutation.columns)

        curr_table = self.datatables[mutation.studytable_id]

        for row in rowwise_data:
            index_params = [k == row[k.name] for k in curr_table.primary_key]
            
            exists = session.execute( # type:ignore
                select(curr_table).where(*index_params)
            ).one_or_none()

            if exists:
                session.execute( # type:ignore
                    update(curr_table)
                        .where(*index_params)
                        .values(row)
                )
            else:
                session.execute( # type: ignore
                    insert(curr_table)
                        .values(row)
                )

        session.commit()