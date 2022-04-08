from __future__ import annotations
from time import time
from ..settings import ProjectSettings
from ..repo.study import StudyRepoReader, StudyRepo
from ..domain.value.base import ImmutableBaseModel


class StudyRepoManager(ImmutableBaseModel):
    settings = ProjectSettings()

    def load_repo_readonly(self) -> StudyRepoReader:
        return StudyRepoReader(self.settings.everything_database_path())

    def load_repo(self) -> StudyRepo:
        self.settings.study_repo_dir.mkdir(exist_ok=True, parents=True)
        oldpath = self.settings.everything_database_path()
        if (oldpath.exists()):
            oldpath.rename(oldpath.with_name(".{}.{}".format(oldpath.name, int(time()))))
        return StudyRepo(self.settings.everything_database_path())