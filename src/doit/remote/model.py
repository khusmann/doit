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

### Blob

class SourceColumnInfo(BaseModel):
    name: str
    prompt: str

class QualtricsSourceInfo(BaseModel):
    type: t.Literal['qualtrics']
    remote_id: str
    data_checksum: str
    schema_checksum: str

TableSourceInfo = QualtricsSourceInfo

class BlobInfo(BaseModel):
    fetch_date_utc: datetime
    title: str
    source_info: TableSourceInfo
    columns: t.Tuple[SourceColumnInfo, ...]

LazyBlobData = t.Mapping[str, t.Callable[[], bytes]]

class LazyBlob(t.NamedTuple):
    info: BlobInfo
    lazy_data: LazyBlobData

class Blob(t.NamedTuple):
    info: BlobInfo
    data: t.Mapping[str, bytes]