import typing as t
from datetime import datetime
from pathlib import Path
from .common import *

### RemoteTable

class RemoteTableListing(ImmutableBaseModel):
    uri: str
    title: str

class RemoteTable(ImmutableBaseModel):
    service: RemoteServiceName
    id: str

    @property
    def uri(self) -> str:
        return "{}://{}".format(self.service, self.id)

### UnsafeTable

class TableFetchInfo(ImmutableBaseModel):
    service: RemoteServiceName
    title: str
    last_update_check: datetime
    last_updated: datetime
    # SHA1?

class TableFileInfo(ImmutableBaseModel):
    format: SourceFormatType
    remote: RemoteTable
    data_path: Path
    schema_path: Path

class ColumnImport(ImmutableBaseModel):
    type: t.Literal['safe_bool', 'unsafe_numeric_text', 'safe_text', 'safe_ordinal', 'unsafe_text']
    source_column_name: SourceColumnName
    prompt: str
    values: t.Tuple[t.Any, ...]

class UnsafeTable(ImmutableBaseModel):
    instrument_name: InstrumentName
    fetch_info: TableFetchInfo
    file_info: TableFileInfo
    columns: t.Tuple[ColumnImport, ...]