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
    service: RemoteServiceName
    id: str

    @property
    def uri(self) -> str:
        return "{}://{}".format(self.service, self.id)

### TableImport

class ColumnImport(ImmutableBaseModel):
    type: t.Literal['safe_bool', 'unsafe_numeric_text', 'safe_text', 'safe_ordinal', 'unsafe_text']
    column_id: SourceColumnName
    prompt: str
    values: t.Tuple[t.Any, ...]

class TableImport(ImmutableBaseModel):
    title: str
    columns: t.Mapping[SourceColumnName, ColumnImport]

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

class UnsafeSourceTable(ImmutableBaseModel):
    instrument_id: InstrumentName
    fetch_info: TableFetchInfo
    file_info: TableFileInfo
    table: TableImport

### SafeTable

ColumnT = t.TypeVar('ColumnT', bound=SourceColumnTypeStr)
DataT = t.TypeVar('DataT', bound=SourceColumnDataType)

class SourceColumnMeta(ImmutableBaseModel):
    column_id: SourceColumnName
    type: SourceColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str]

class SourceColumnBase(ImmutableGenericModel, t.Generic[ColumnT, DataT]):
    column_id: SourceColumnName
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

def new_source_column(column_id: SourceColumnName, meta: SourceColumnMeta, column_type: SourceColumnTypeStr, values: t.Sequence[t.Any | None]) -> SourceColumn:
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
    instrument_id: InstrumentName
    source_info: TableFetchInfo
    columns: t.Mapping[SourceColumnName, SourceColumnMeta]

class SourceTable(ImmutableBaseModel):
    instrument_id: InstrumentName
    meta: SourceTableMeta
    columns: t.Mapping[SourceColumnName, SourceColumn]

### LinkedTable

class LinkedColumn(ImmutableBaseModel):
    column_id: SourceColumnName
    type: SourceColumnTypeStr
    values: t.Tuple[t.Any, ...]

class LinkedTable(ImmutableBaseModel):
    instrument_id: InstrumentName
    table_id: StudyTableName
    columns: t.Tuple[LinkedColumn, ...]