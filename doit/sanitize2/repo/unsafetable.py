import typing as t
from pydantic import BaseModel, BaseSettings

from urllib.parse import urlparse, ParseResult
from pathlib import Path
from time import time

from ..domain.value import UnsafeTable, UnsafeTableSourceInfo, InstrumentId, RemoteInfo
from ..io.remote import fetch_remote_table
from ..io.unsafetable import read_unsafe_table

class UnsafeTableRepoSettings(BaseSettings):
    repo_dir = Path("./build/unsafe/sources")

    def workdir(self, instrument_id: str) -> Path:
        return self.repo_dir / instrument_id

    def info_file(self, instrument_id: str) -> Path:
        return (self.workdir(instrument_id) / instrument_id).with_suffix(".json")

    class Config(BaseSettings.Config):
        env_prefix = "unsafetable_"  

class UnsafeTableRepo(BaseModel):
    settings = UnsafeTableRepoSettings()

    def query(self, instrument_id: InstrumentId) -> UnsafeTable:
        info = self.query_source_info(instrument_id)
        return read_unsafe_table(info.format, info.data_path, info.schema_path)

    def query_source_info(self, instrument_id: InstrumentId) -> UnsafeTableSourceInfo:
        return UnsafeTableSourceInfo.parse_file(self.settings.info_file(instrument_id))

    def fetch(self, instrument_id: InstrumentId) -> None:
        info = self.query_source_info(instrument_id)
        return fetch_remote_table(info.remote_info, info.data_path, info.schema_path)

    def add(self, instrument_id: InstrumentId, uri: str) -> None:
        self.settings.workdir(instrument_id).mkdir(exist_ok=True, parents=True)
        match urlparse(uri):
            case ParseResult(scheme="qualtrics", netloc=remote_id):
                new_source =  UnsafeTableSourceInfo(
                    instrument_id=instrument_id,
                    remote_info=RemoteInfo(
                        service="qualtrics",
                        id=remote_id,
                    ),
                    format="qualtrics",
                    data_path=self.settings.workdir(instrument_id) / "qualtrics-data.json",
                    schema_path=self.settings.workdir(instrument_id) / "qualtrics-schema.json",
                )
            case _:
                raise Exception("Unrecognized uri: {}".format(uri))

        with open(self.settings.info_file(instrument_id), 'w') as f:
            f.write(new_source.json())

    def rm(self, instrument_id: InstrumentId) -> None:
        oldfile = self.settings.workdir(instrument_id)
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def tables(self) -> t.List[InstrumentId]:
        return [ InstrumentId(i.name) for i in self.settings.repo_dir.iterdir() if i.is_dir() and i.name[0] != '.' ]