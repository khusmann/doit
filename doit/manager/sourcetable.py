from pydantic import BaseSettings
from pathlib import Path
from time import time

from ..repo.sourcetable import SourceTableRepoWriter, SourceTableRepoReader

class SourceTableRepoManager(BaseSettings):
    repo_dir = Path("./build/safe/sanitized")
    database_filename = "peak-sanitized.db"

    def database_path(self):
        return self.repo_dir / self.database_filename

    class Config(BaseSettings.Config):
        env_prefix = "safetable_"

    def load_reader(self) -> SourceTableRepoReader:
        return SourceTableRepoReader(self.database_path())

    def load_writer(self) -> SourceTableRepoWriter:
        self.repo_dir.mkdir(exist_ok=True, parents=True)
        oldpath = self.database_path()
        if (oldpath.exists()):
            oldpath.rename(oldpath.with_name(".{}.{}".format(oldpath.name, int(time()))))
        return SourceTableRepoWriter(self.database_path())