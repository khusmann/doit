from __future__ import annotations
from time import time
from ..settings import ProjectSettings
from ..repo.sourcetable import SourceTableRepo, SourceTableRepoReader
from ..domain.value import *


class SourceTableRepoManager(ImmutableBaseModel):
    settings = ProjectSettings()

    def load_repo_readonly(self) -> SourceTableRepoReader:
        return SourceTableRepoReader(self.settings.source_database_path())

    def load_repo(self) -> SourceTableRepo:
        self.settings.safe_source_repo_dir.mkdir(exist_ok=True, parents=True)
        oldpath = self.settings.source_database_path()
        if (oldpath.exists()):
            oldpath.rename(oldpath.with_name(".{}.{}".format(oldpath.name, int(time()))))
        return SourceTableRepo(self.settings.source_database_path())