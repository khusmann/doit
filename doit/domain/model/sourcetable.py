import typing as t
from ..value import *


ColumnT = t.TypeVar('ColumnT', bound=SourceColumnTypeStr)
DataT = t.TypeVar('DataT', bound=SourceColumnDataType)

class SourceColumnInfo(ImmutableBaseModel):
    source_column_name: SourceColumnName
    type: SourceColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str]

class SourceColumnBase(ImmutableGenericModel, t.Generic[ColumnT, DataT]):
    source_column_name: SourceColumnName
    meta: SourceColumnInfo
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

def new_source_column(source_column_name: SourceColumnName, meta: SourceColumnInfo, column_type: SourceColumnTypeStr, values: t.Sequence[t.Any | None]) -> SourceColumn:
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
        source_column_name=source_column_name,
        meta=meta,
        type=column_type,
        values=values
    )

class SourceTableInfo(ImmutableBaseModel):
    instrument_name: InstrumentName
    source_info: TableFetchInfo
    columns: t.Mapping[SourceColumnName, SourceColumnInfo]

class SourceTable(ImmutableBaseModel):
    instrument_name: InstrumentName
    meta: SourceTableInfo
    columns: t.Mapping[SourceColumnName, SourceColumn]