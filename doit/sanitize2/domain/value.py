import typing as t
from datetime import datetime
from pydantic import BaseModel, Field
from pathlib import Path

InstrumentId = t.NewType('InstrumentId', str)

ColumnId = t.NewType('ColumnId', str)

FormatType = t.Literal['qualtrics']

RemoteService = t.Literal['qualtrics']

class RemoteInfo(BaseModel):
    service: RemoteService
    id: str

class RemoteTableListing(BaseModel):
    uri: str
    title: str

class UnsafeTableSourceInfo(BaseModel):
    instrument_id: InstrumentId
    last_update_check: t.Optional[datetime]
    last_updated: t.Optional[datetime]
    remote_info: RemoteInfo
    format: FormatType
    data_path: Path
    schema_path: Path

    @property
    def uri(self):
        return "{}://{}".format(self.remote_info.service, self.remote_info.id)

class ColumnDataBase(BaseModel):
    column_id: ColumnId
    prompt: str

class SafeBoolColumnData(ColumnDataBase):
    status: t.Literal['safe']
    type: t.Literal['bool']
    values: t.List[t.Optional[bool]]

class SafeOrdinalColumnData(ColumnDataBase):
    status: t.Literal['safe']
    type: t.Literal['ordinal']
    values: t.List[t.Optional[str]]
    codes: t.Mapping[str, int]

class SafeRealColumnData(ColumnDataBase):
    status: t.Literal['safe']
    type: t.Literal['real']
    values: t.List[t.Optional[float]]

class SafeIntegerColumnData(ColumnDataBase):
    status: t.Literal['safe']
    type: t.Literal['integer']
    values: t.List[t.Optional[int]]

class UnsafeNumericTextColumnData(ColumnDataBase):
    status: t.Literal['unsafe']
    type: t.Literal['numeric_text']
    values: t.List[t.Optional[str]]

class SafeTextColumnData(ColumnDataBase):
    status: t.Literal['safe']
    type: t.Literal['text']
    values: t.List[t.Optional[str]]

class UnsafeTextColumnData(ColumnDataBase):
    status: t.Literal['unsafe']
    type: t.Literal['text']
    values: t.List[t.Optional[str]]

UnsafeColumnData = t.Annotated[
    t.Union[
        UnsafeTextColumnData,
        UnsafeNumericTextColumnData
    ],
    Field(discriminator='type')
]

SafeColumnData = t.Annotated[
    t.Union[
        SafeTextColumnData,
        SafeIntegerColumnData,
        SafeRealColumnData,
        SafeOrdinalColumnData,
        SafeBoolColumnData
    ],
    Field(discriminator='type')
]

ColumnData = t.Annotated[
    t.Union[
        UnsafeColumnData,
        SafeColumnData,
    ],
    Field(discriminator='status')
]

class UnsafeTable(BaseModel):
    title: str
    columns: t.Mapping[str, ColumnData]

class SafeTable(BaseModel):
    title: str
    columns: t.Mapping[str, SafeColumnData]