import typing as t
from pydantic import BaseSettings

from urllib.parse import urlparse, ParseResult
from pathlib import Path
from time import time

from ..domain.value import (
    ImmutableBaseModel,
    UnsafeSourceTable,
    InstrumentName,
    RemoteTable,
    TableFileInfo,
    TableFetchInfo
)

from ..remote import fetch_remote_table
from ..io import read_unsafe_table_data

class UnsafeTableRepoSettings(BaseSettings):
    repo_dir = Path("./build/unsafe/sources")

    def workdir(self, instrument_id: str) -> Path:
        return self.repo_dir / instrument_id

    def fileinfo_file(self, instrument_id: str) -> Path:
        return (self.workdir(instrument_id) / instrument_id).with_suffix(".json")

    def fetchinfo_file(self, instrument_id: str) -> Path:
        return (self.workdir(instrument_id) / instrument_id).with_suffix(".fetch.json")

    class Config(BaseSettings.Config):
        env_prefix = "unsafetable_"  

class UnsafeTableManager(ImmutableBaseModel):
    settings = UnsafeTableRepoSettings()

    def load_table(self, instrument_id: InstrumentName) -> UnsafeSourceTable:
        file_info = self.load_file_info(instrument_id)
        fetch_info = self.load_fetch_info(instrument_id)
        table = read_unsafe_table_data(file_info)
        return UnsafeSourceTable(
            instrument_id=instrument_id,
            file_info=file_info,
            fetch_info=fetch_info,
            table=table,
        )

    def load_file_info(self, instrument_id: InstrumentName) -> TableFileInfo:
        return TableFileInfo.parse_file(self.settings.fileinfo_file(instrument_id))

    def load_fetch_info(self, instrument_id: InstrumentName) -> TableFetchInfo:
        return TableFetchInfo.parse_file(self.settings.fetchinfo_file(instrument_id))

    def fetch(self, instrument_id: InstrumentName) -> None:
        file_info = self.load_file_info(instrument_id)
        fetch_info = fetch_remote_table(file_info)

        with open(self.settings.fetchinfo_file(instrument_id), 'w') as f:
            f.write(fetch_info.json())

    def add(self, instrument_id: InstrumentName, uri: str) -> None:
        self.settings.workdir(instrument_id).mkdir(exist_ok=True, parents=True)
        match urlparse(uri):
            case ParseResult(scheme="qualtrics", netloc=remote_id):
                file_info =  TableFileInfo(
                    remote=RemoteTable(
                        service="qualtrics",
                        id=remote_id,
                    ),
                    format="qualtrics",
                    data_path=self.settings.workdir(instrument_id) / "qualtrics-data.json",
                    schema_path=self.settings.workdir(instrument_id) / "qualtrics-schema.json",
                )
            case _:
                raise Exception("Unrecognized uri: {}".format(uri))

        with open(self.settings.fileinfo_file(instrument_id), 'w') as f:
            f.write(file_info.json())

    def rm(self, instrument_id: InstrumentName) -> None:
        oldfile = self.settings.workdir(instrument_id)
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def tables(self) -> t.List[InstrumentName]:
        return [ InstrumentName(i.name) for i in self.settings.repo_dir.iterdir() if i.is_dir() and i.name[0] != '.' ]