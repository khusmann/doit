from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from ...domain.value.studyspec import *
from ...domain.service import *

from .model import *

class StudyRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=True)

    # def query...

class StudyRepo(StudyRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=True)
        Base.metadata.create_all(self.engine)

    def setup(self, study_spec: StudySpec):
        self._mutate(index_columns_from_spec(study_spec.config.indices))
        self._mutate(measures_from_spec(study_spec.measures))

    def _mutate(self, mutations: t.Sequence[StudyMutation]):
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

        session.commit()

        return self
