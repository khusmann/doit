import typing as t
from datetime import datetime
from pydantic import BaseModel
from pathlib import Path

class RemoteTableInfo(BaseModel):
    uri: str
    title: str

class UnsafeTableSourceInfo(BaseModel):
    instrument_id: str
    last_update_check: t.Optional[datetime]
    last_updated: t.Optional[datetime]
    remote_id: t.Tuple[str, str]
    format: str
    data_path: Path
    schema_path: Path

    @property
    def uri(self):
        return "{}://{}".format(self.remote_id[0], self.remote_id[1])

class UnsafeDataColumn(BaseModel):
    column_id: str
    prompt: str
    type: t.Literal['category', 'string', 'numeric', 'bool']
    data: t.Union[t.List[t.Optional[str]], t.List[t.Optional[bool]]]
    # TODO: Add verification that bool -> list[bool]

class UnsafeTable(BaseModel):
    title: str
    columns: t.Mapping[str, UnsafeDataColumn]

class SourceColumn(BaseModel):
    column_id: str
    prompt: str
    type: t.Literal['category', 'string', 'numeric', 'bool']
    data: t.Union[t.List[t.Optional[str]], t.List[t.Optional[bool]]]
    # TODO: Add verification that bool -> list[bool]

class UnsafeSourceColumn(SourceColumn):
    status: t.Literal['unsafe']

class SafeSourceColumn(SourceColumn):
    status: t.Literal['safe']
