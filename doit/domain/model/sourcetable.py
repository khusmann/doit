import typing as t
from ..value import *

class SourceColumnInfo(ImmutableBaseModel):
    source_column_info_id: t.Optional[SourceColumnInfoId]
    source_column_name: SourceColumnName
    type: SourceColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str] # TODO

class SourceTableInfo(ImmutableBaseModel):
    source_table_info_id: t.Optional[SourceTableInfoId]
    instrument_name: InstrumentName
    source_info: t.Optional[str] # TODO
    columns: t.Mapping[SourceColumnName, SourceColumnInfo]

class SourceColumnData(ImmutableBaseModel):
    source_column_name: SourceColumnName
    type: SourceColumnTypeStr
    values: t.Tuple[t.Any | None, ...]

class SourceTable(ImmutableBaseModel):
    instrument_name: InstrumentName
    info: SourceTableInfo
    data: t.Mapping[SourceColumnName, SourceColumnData]