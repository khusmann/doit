import typing as t
from pydantic import BaseModel
from datetime import datetime

### Listing

class RemoteTableListing(t.NamedTuple):
    uri: str
    title: str

class LocalTableListing(t.NamedTuple):
    name: str
    title: str
    uri: str

### Blob

class SourceColumnInfo(BaseModel):
    name: str
    prompt: str

class QualtricsSourceInfo(BaseModel):
    type: t.Literal['qualtrics']
    remote_id: str
    data_checksum: str
    schema_checksum: str

    @property
    def uri(self):
        return "qualtrics://" + self.remote_id

class WearitSourceInfo(BaseModel):
    type: t.Literal['wearit']
    data_checksum: str
    schema_checksum: str

    @property
    def uri(self):
        return "Not implemented"

TableSourceInfo = t.Union[
    QualtricsSourceInfo,
    WearitSourceInfo,
]

class BlobInfo(BaseModel):
    fetch_date_utc: datetime
    title: str
    source_info: TableSourceInfo
    columns: t.Tuple[SourceColumnInfo, ...]

class Blob(t.NamedTuple):
    info: BlobInfo
    lazydata: t.Mapping[str, t.Callable[[], bytes]]