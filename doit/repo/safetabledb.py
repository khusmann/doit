from pydantic import BaseSettings, BaseModel
from pathlib import Path
from time import time

from ..io.safetabledb import SafeTableDbWriter, SafeTableDbReader

class SafeTableRepoDbSettings(BaseSettings):
    repo_dir = Path("./build/safe/sanitized")
    database_filename = "peak-sanitized.db"

    def database_path(self):
        return self.repo_dir / self.database_filename

    class Config(BaseSettings.Config):
        env_prefix = "safetable_"


class SafeTableDbRepo(BaseModel):
    settings = SafeTableRepoDbSettings()

    def query(self) -> SafeTableDbReader:
        return SafeTableDbReader(self.settings.database_path())

    def new_db(self) -> SafeTableDbWriter:
        self.settings.repo_dir.mkdir(exist_ok=True, parents=True)
        oldpath = self.settings.database_path()
        if (oldpath.exists()):
            oldpath.rename(oldpath.with_name(".{}.{}".format(oldpath.name, int(time()))))
        return SafeTableDbWriter(self.settings.database_path())