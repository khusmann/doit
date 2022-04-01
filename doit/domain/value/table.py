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

class TableFetchInfo(ImmutableBaseModel):
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

class UnsafeSourceTable(ImmutableBaseModel):
    instrument_id: InstrumentId
    fetch_info: TableFetchInfo
    file_info: TableFileInfo
    table: TableImport

### SafeTable

ColumnT = t.TypeVar('ColumnT', bound=ColumnTypeStr)
DataT = t.TypeVar('DataT', bound=ColumnDataType)

class SourceColumnMeta(ImmutableBaseModel):
    column_id: ColumnId
    type: ColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str]

class SourceColumnBase(ImmutableGenericModel, t.Generic[ColumnT, DataT]):
    column_id: ColumnId
    meta: SourceColumnMeta
    type: ColumnT
    values: t.Tuple[DataT | None, ...]

SourceColumn = t.Annotated[
    t.Union[
        SourceColumnBase[t.Literal['bool'], StrictBool],
        SourceColumnBase[t.Literal['ordinal'], StrictStr],
        SourceColumnBase[t.Literal['real'], StrictFloat],
        SourceColumnBase[t.Literal['text'], StrictStr],
        SourceColumnBase[t.Literal['integer'], StrictInt],
    ], Field(discriminator='type')
]

def new_source_column(column_id: ColumnId, meta: SourceColumnMeta, column_type: ColumnTypeStr, values: t.Sequence[t.Any | None]) -> SourceColumn:
    match column_type:
        case 'bool':
            new_func = SourceColumnBase[t.Literal['bool'], StrictBool]
        case 'ordinal':
            new_func = SourceColumnBase[t.Literal['ordinal'], StrictStr]
        case 'real':
            new_func = SourceColumnBase[t.Literal['real'], StrictFloat]
        case 'text':
            new_func = SourceColumnBase[t.Literal['text'], StrictStr]
        case 'integer':
            new_func = SourceColumnBase[t.Literal['integer'], StrictInt]

    return new_func(
        column_id=column_id,
        meta=meta,
        type=column_type,
        values=values
    )

class SourceTableMeta(ImmutableBaseModel):
    instrument_id: InstrumentId
    source_info: TableFetchInfo
    columns: t.Mapping[ColumnId, SourceColumnMeta]

class SourceTable(ImmutableBaseModel):
    instrument_id: InstrumentId
    meta: SourceTableMeta
    columns: t.Mapping[ColumnId, SourceColumn]

### LinkedTable

class LinkedColumn(ImmutableBaseModel):
    column_id: ColumnId
    type: ColumnTypeStr
    values: t.Tuple[t.Any, ...]

class LinkedTable(ImmutableBaseModel):
    instrument_id: InstrumentId
    table_id: TableId
    columns: t.Tuple[LinkedColumn, ...]