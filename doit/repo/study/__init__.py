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
        measure_mutations, _ = measures_from_spec(study_spec.measures)
        self._mutate(measure_mutations)

    def _mutate(self, mutations: StudyMutationList):
        session = Session(self.engine)

        for mutation in mutations:
            match mutation:
                case AddEntityMutation():
                    session.add(entity_to_sql(mutation.entity)) # type:ignore
                case ConnectNodeToTable():
                    print("Not implemented yet")

        session.commit()

        return self
