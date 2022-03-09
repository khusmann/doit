import typing as t
from pydantic import BaseSettings

from urllib.parse import urlparse, ParseResult
from pathlib import Path
from time import time
from datetime import datetime

from ..domain.value import (
    ImmutableBaseModel,
    UnsafeTable,
    UnsafeTableMeta,
    InstrumentId,
    RemoteTableId,
    TableFileInfo,
    TableSourceInfo
)

from ..io.remote import fetch_remote_table
from ..io.unsafetable import read_unsafe_table_data

class UnsafeTableRepoSettings(BaseSettings):
    repo_dir = Path("./build/unsafe/sources")

    def workdir(self, instrument_id: str) -> Path:
        return self.repo_dir / instrument_id

    def meta_file(self, instrument_id: str) -> Path:
        return (self.workdir(instrument_id) / instrument_id).with_suffix(".json")

    class Config(BaseSettings.Config):
        env_prefix = "unsafetable_"  

class UnsafeTableRepo(ImmutableBaseModel):
    settings = UnsafeTableRepoSettings()

    def query(self, instrument_id: InstrumentId) -> UnsafeTable:
        meta = self.query_meta(instrument_id)
        data = read_unsafe_table_data(meta.file_info)
        return UnsafeTable(
            instrument_id=instrument_id,
            meta=meta,
            columns=data.columns,
        )

    def query_meta(self, instrument_id: InstrumentId) -> UnsafeTableMeta:
        return UnsafeTableMeta.parse_file(self.settings.meta_file(instrument_id))

    def fetch(self, instrument_id: InstrumentId) -> None:
        meta = self.query_meta(instrument_id)
        fetch_remote_table(meta.file_info)
        data = read_unsafe_table_data(meta.file_info) # TODO: Only the schema really needs to be loaded...

        meta.source_info = TableSourceInfo(
            service = meta.file_info.remote_id.service,
            last_update_check = datetime.now(),
            last_updated = datetime.now(),
            title = data.title,
        )

        with open(self.settings.meta_file(instrument_id), 'w') as f:
            f.write(meta.json())

    def add(self, instrument_id: InstrumentId, uri: str) -> None:
        self.settings.workdir(instrument_id).mkdir(exist_ok=True, parents=True)
        match urlparse(uri):
            case ParseResult(scheme="qualtrics", netloc=remote_id):
                new_meta =  UnsafeTableMeta(
                    instrument_id=instrument_id,
                    file_info=TableFileInfo(
                        remote_id=RemoteTableId(
                            service="qualtrics",
                            id=remote_id,
                        ),
                        format="qualtrics",
                        data_path=self.settings.workdir(instrument_id) / "qualtrics-data.json",
                        schema_path=self.settings.workdir(instrument_id) / "qualtrics-schema.json",
                    ),
                )
            case _:
                raise Exception("Unrecognized uri: {}".format(uri))

        with open(self.settings.meta_file(instrument_id), 'w') as f:
            f.write(new_meta.json())

    def rm(self, instrument_id: InstrumentId) -> None:
        oldfile = self.settings.workdir(instrument_id)
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def tables(self) -> t.List[InstrumentId]:
        return [ InstrumentId(i.name) for i in self.settings.repo_dir.iterdir() if i.is_dir() and i.name[0] != '.' ]