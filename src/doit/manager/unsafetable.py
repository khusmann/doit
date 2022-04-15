import typing as t
from urllib.parse import urlparse, ParseResult
from time import time

from ..domain.value import *
from ..settings import ProjectSettings
from ..remote import fetch_remote_table
from ..io import read_unsafe_table_data

class UnsafeTableManager(ImmutableBaseModel):
    settings = ProjectSettings()

    def load_unsafe_table(self, instrument_name: InstrumentName) -> UnsafeTable:
        file_info = self.load_file_info(instrument_name)
        source_table_info = self.load_fetch_info(instrument_name)
        columns = read_unsafe_table_data(file_info)
        return UnsafeTable(
            instrument_name=instrument_name,
            file_info=file_info,
            source_table_info=source_table_info,
            columns={ column.source_column_name: column for column in columns },
        )

    def load_file_info(self, instrument_name: InstrumentName) -> TableFileInfo:
        return TableFileInfo.parse_file(self.settings.unsafe_table_fileinfo_file(instrument_name))

    def load_fetch_info(self, instrument_id: InstrumentName) -> SourceTableInfo:
        return SourceTableInfo.parse_file(self.settings.unsafe_table_fetchinfo_file(instrument_id))

    def fetch(
        self,
        instrument_name: InstrumentName,
        progress_callback: t.Callable[[int], None] = lambda _: None,
    ) -> UnsafeTable:
        file_info = self.load_file_info(instrument_name)
        fetch_info = fetch_remote_table(file_info, progress_callback)

        with open(self.settings.unsafe_table_fetchinfo_file(instrument_name), 'w') as f:
            f.write(fetch_info.json())

        return self.load_unsafe_table(instrument_name)

    def add(self, instrument_id: InstrumentName, uri: str) -> None:
        self.settings.unsafe_table_workdir(instrument_id).mkdir(exist_ok=True, parents=True)
        match urlparse(uri):
            case ParseResult(scheme="qualtrics", netloc=remote_id):
                file_info =  TableFileInfo(
                    remote=RemoteTable(
                        service="qualtrics",
                        id=remote_id,
                    ),
                    format="qualtrics",
                    data_path=self.settings.unsafe_table_workdir(instrument_id) / "qualtrics-data.json",
                    schema_path=self.settings.unsafe_table_workdir(instrument_id) / "qualtrics-schema.json",
                )
            case _:
                raise Exception("Unrecognized uri: {}".format(uri))

        with open(self.settings.unsafe_table_fileinfo_file(instrument_id), 'w') as f:
            f.write(file_info.json())

    def rm(self, instrument_id: InstrumentName) -> None:
        oldfile = self.settings.unsafe_table_workdir(instrument_id)
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def tables(self) -> t.List[InstrumentName]:
        return [InstrumentName(i) for i in self.settings.get_unsafe_table_names()]