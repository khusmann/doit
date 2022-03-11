import typing as t
from datetime import datetime
from pathlib import Path
from .common import *
from pydantic import Field

### RemoteTable

class RemoteTableListing(ImmutableBaseModel):
    uri: str
    title: str

class RemoteTable(ImmutableBaseModel):
    service: RemoteService
    id: str

    @property
    def uri(self) -> str:
        return "{}://{}".format(self.service, self.id)

### TableImport

class ColumnImport(ImmutableBaseModel):
    type: t.Literal['safe_bool', 'unsafe_numeric_text', 'safe_text', 'safe_ordinal', 'unsafe_text']
    column_id: ColumnId
    prompt: str
    values: t.Tuple[t.Any, ...]

class TableImport(ImmutableBaseModel):
    title: str
    columns: t.Mapping[ColumnId, ColumnImport]

### UnsafeTable

class TableSourceInfo(ImmutableBaseModel):
    service: RemoteService
    title: str
    last_update_check: datetime
    last_updated: datetime
    # SHA1?

class TableFileInfo(ImmutableBaseModel):
    format: FormatType
    remote: RemoteTable
    data_path: Path
    schema_path: Path

class UnsafeTableMeta(ImmutableBaseModel):
    instrument_id: InstrumentId
    source_info: t.Optional[TableSourceInfo]
    file_info: TableFileInfo

class UnsafeTable(ImmutableBaseModel):
    instrument_id: InstrumentId
    meta: UnsafeTableMeta
    columns: t.Mapping[ColumnId, ColumnImport]

### SafeTable

ColumnT = t.TypeVar('ColumnT', bound=ColumnTypeStr)
DataT = t.TypeVar('DataT', bound=ColumnDataType)

class SafeColumnMeta(ImmutableBaseModel):
    column_id: ColumnId
    type: ColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str]

class SafeColumnBase(ImmutableGenericModel, t.Generic[ColumnT, DataT]):
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

class SafeTableMeta(ImmutableBaseModel):
    instrument_id: InstrumentId
    source_info: TableSourceInfo
    columns: t.Mapping[ColumnId, SafeColumnMeta]

class SafeTable(ImmutableBaseModel):
    instrument_id: InstrumentId
    meta: SafeTableMeta
    columns: t.Mapping[ColumnId, SafeColumn]

### LinkedTable