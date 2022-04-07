import typing as t
from urllib.parse import urlparse, ParseResult
from time import time
from .settings import ProjectSettings

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

class UnsafeTableManager(ImmutableBaseModel):
    settings = ProjectSettings()

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
        return TableFileInfo.parse_file(self.settings.unsafe_source_fileinfo_file(instrument_id))

    def load_fetch_info(self, instrument_id: InstrumentName) -> TableFetchInfo:
        return TableFetchInfo.parse_file(self.settings.unsafe_source_fetchinfo_file(instrument_id))

    def fetch(self, instrument_id: InstrumentName) -> None:
        file_info = self.load_file_info(instrument_id)
        fetch_info = fetch_remote_table(file_info)

        with open(self.settings.unsafe_source_fetchinfo_file(instrument_id), 'w') as f:
            f.write(fetch_info.json())

    def add(self, instrument_id: InstrumentName, uri: str) -> None:
        self.settings.unsafe_source_workdir(instrument_id).mkdir(exist_ok=True, parents=True)
        match urlparse(uri):
            case ParseResult(scheme="qualtrics", netloc=remote_id):
                file_info =  TableFileInfo(
                    remote=RemoteTable(
                        service="qualtrics",
                        id=remote_id,
                    ),
                    format="qualtrics",
                    data_path=self.settings.unsafe_source_workdir(instrument_id) / "qualtrics-data.json",
                    schema_path=self.settings.unsafe_source_workdir(instrument_id) / "qualtrics-schema.json",
                )
            case _:
                raise Exception("Unrecognized uri: {}".format(uri))

        with open(self.settings.unsafe_source_fileinfo_file(instrument_id), 'w') as f:
            f.write(file_info.json())

    def rm(self, instrument_id: InstrumentName) -> None:
        oldfile = self.settings.unsafe_source_workdir(instrument_id)
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def tables(self) -> t.List[InstrumentName]:
        return [ InstrumentName(i.name) for i in self.settings.unsafe_source_repo_dir.iterdir() if i.is_dir() and i.name[0] != '.' ]