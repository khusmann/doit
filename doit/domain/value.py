from __future__ import annotations
import typing as t
from datetime import datetime
from pydantic import (
    BaseModel,
    Field,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
)
from pydantic.generics import GenericModel
from pathlib import Path

### 

InstrumentId = t.NewType('InstrumentId', str)
ColumnId = t.NewType('ColumnId', str)

### Remote Service

RemoteService = t.Literal['qualtrics']

class RemoteTableId(BaseModel):
    service: RemoteService
    id: str

    @property
    def uri(self) -> str:
        return "{}://{}".format(self.service, self.id)

class RemoteTableListing(BaseModel):
    uri: str
    title: str


### ColumnImport

ColumnImportTypeStr = t.Literal['safe_bool', 'unsafe_numeric_text', 'safe_text', 'safe_ordinal', 'unsafe_text']
FormatType = t.Literal['qualtrics']

class ColumnImport(BaseModel):
    type: ColumnImportTypeStr
    column_id: ColumnId
    prompt: str
    values: t.Tuple[t.Any, ...]

class TableImport(BaseModel):
    title: str
    columns: t.Mapping[ColumnId, ColumnImport]

class TableSourceInfo(BaseModel):
    service: RemoteService
    title: str
    last_update_check: datetime
    last_updated: datetime
    # SHA1?

class TableFileInfo(BaseModel):
    format: FormatType
    remote_id: RemoteTableId
    data_path: Path
    schema_path: Path

class UnsafeTableMeta(BaseModel):
    instrument_id: InstrumentId
    source_info: t.Optional[TableSourceInfo]
    file_info: TableFileInfo

class UnsafeTable(BaseModel):
    instrument_id: InstrumentId
    meta: UnsafeTableMeta
    columns: t.Mapping[ColumnId, ColumnImport]


### Safe Table

ColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
ColumnDataType = t.Union[StrictBool, StrictStr, StrictFloat, StrictInt]

ColumnT = t.TypeVar('ColumnT', bound=ColumnTypeStr)
DataT = t.TypeVar('DataT', bound=ColumnDataType)

class SafeColumnMeta(BaseModel):
    column_id: ColumnId
    type: ColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str]

class SafeColumnBase(GenericModel, t.Generic[ColumnT, DataT]):
    column_id: ColumnId
    meta: SafeColumnMeta
    type: ColumnT
    values: t.Tuple[DataT | None, ...]

SafeColumn = t.Annotated[
    t.Union[
        SafeColumnBase[t.Literal['bool'], StrictBool],
        SafeColumnBase[t.Literal['ordinal'], StrictStr],
        SafeColumnBase[t.Literal['real'], StrictFloat],
        SafeColumnBase[t.Literal['text'], StrictStr],
        SafeColumnBase[t.Literal['integer'], StrictInt],
    ], Field(discriminator='type')
]

def new_safe_column(column_id: ColumnId, meta: SafeColumnMeta, column_type: ColumnTypeStr, values: t.Sequence[t.Any | None]) -> SafeColumn:
    match column_type:
        case 'bool':
            new_func = SafeColumnBase[t.Literal['bool'], StrictBool]
        case 'ordinal':
            new_func = SafeColumnBase[t.Literal['ordinal'], StrictStr]
        case 'real':
            new_func = SafeColumnBase[t.Literal['real'], StrictFloat]
        case 'text':
            new_func = SafeColumnBase[t.Literal['text'], StrictStr]
        case 'integer':
            new_func = SafeColumnBase[t.Literal['integer'], StrictInt]

    return new_func(
        column_id=column_id,
        meta=meta,
        type=column_type,
        values=values
    )

class SafeTableMeta(BaseModel):
    instrument_id: InstrumentId
    source_info: TableSourceInfo
    columns: t.Mapping[ColumnId, SafeColumnMeta]

class SafeTable(BaseModel):
    instrument_id: InstrumentId
    meta: SafeTableMeta
    columns: t.Mapping[ColumnId, SafeColumn]