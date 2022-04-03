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

class StudyRepo(StudyRepoReader):
    def __init__(self, path: Path):
        assert not path.exists()
        self.url = "sqlite:///{}".format(path)
        self.engine = create_engine(self.url, echo=True)
        Base.metadata.create_all(self.engine)

    def add_entities(self, entities: t.Sequence[StudyEntity]):
        session = Session(self.engine)

        for e in entities:
            session.add(sql_lookup[type(e)](e)) # type: ignore

        session.commit()

        return self