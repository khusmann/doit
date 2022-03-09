#import typing as t
from pydantic import BaseSettings
from pathlib import Path
from ..domain.value import SafeTable, ImmutableBaseModel

class StudySpecRepoSettings(BaseSettings):
    instrument_dir = Path("./instruments")
    measure_dir = Path("./measures")

class StudySpecRepo(ImmutableBaseModel):
    settings = StudySpecRepoSettings()

    def query_instruments(self):
        pass

    def query_measures(self):
        pass

    def stub_instrument(self, table: SafeTable):
        pass