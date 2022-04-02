from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine, select

from sqlalchemy.orm import Session 

from ...domain.value.studyspec import *
from ...domain.service import *

from .model import *

class StudyRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=True)

class StudyRepo(StudyRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=True)
        Base.metadata.create_all(self.engine)

    def mutate(self, mutations: t.Sequence[StudyMutation]):
        session = Session(self.engine)

        for mutation in mutations:
            match mutation:
                case AddCodeMapMutator():
                    session.add(CodeMapSql(mutation.codemap)) # type: ignore
                case AddMeasureMutator():
                    session.add(MeasureSql(mutation.measure)) # type: ignore
                case AddMeasureNodeMutator():
                    session.add(MeasureNodeSql(mutation.measure_node)) # type: ignore
                case AddIndexColumnMutator():
                    session.add(IndexColumnSql(mutation.index_column)) # type: ignore
                case AddInstrumentMutator():
                    session.add(InstrumentSql(mutation.instrument)) # type: ignore
                case AddInstrumentNodeMutator():
                    session.add(InstrumentNodeSql(mutation.instrument_node)) # type: ignore
                case AddStudyTableMutator():
                    session.add(StudyTableSql(mutation.studytable)) # type: ignore
                case ConnectColumnToTableMutator():
                    column = _query_column_by_name(session, mutation.column_name)
                    column.studytable_id = mutation.studytable_id
                case ConnectInstrumentNodeToColumnMutator():
                    column = _query_column_by_name(session, mutation.column_name)
                    node = _query_instrument_node_by_id(session, mutation.node_id)

                    match column:
                        case IndexColumnSql():
                            node.index_column_id=column.id
                        case MeasureNodeSql():
                            node.measure_node_id=column.id


        session.commit()

        return self

def _query_instrument_node_by_id(session: Session, node_id: int) -> InstrumentNodeSql:
    node: t.Optional[InstrumentNodeSql] = session.execute( # type: ignore
        select(InstrumentNodeSql).where(InstrumentNodeSql.id == node_id)
    ).scalar_one_or_none()

    if node is None:
        raise Exception("Cannot find instrument node id: {}".format(node_id))

    return node


def _query_column_by_name(session: Session, column_name: str) -> t.Union[IndexColumnSql, MeasureNodeSql]:
    if column_name.startswith("indices."):
        column: t.Optional[IndexColumnSql] = session.execute( # type: ignore
            select(IndexColumnSql).where(IndexColumnSql.name == column_name.removeprefix("indices."))
        ).scalar_one_or_none()
    else:
        column: t.Optional[MeasureNodeSql] = session.execute( # type: ignore
            select(MeasureNodeSql).where(MeasureNodeSql.name == column_name)
        ).scalar_one_or_none()

    if column is None:
        raise Exception("Cannot find column: {}".format(column_name))

    return column

