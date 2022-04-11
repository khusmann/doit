import typing as t
from ..value import *

class SourceColumnInfo(ImmutableBaseModelOrm):
    id: SourceColumnInfoId
    parent_table_id: SourceTableInfoId
    name: SourceColumnName
    type: SourceColumnTypeStr
    prompt: str
    sanitizer_meta: t.Optional[str] # TODO

class SourceTableInfo(ImmutableBaseModelOrm):
    id: SourceTableInfoId
    name: InstrumentName
    source_info: t.Optional[str] # TODO
    columns: t.Dict[SourceColumnName, SourceColumnInfo]

class SourceColumnData(ImmutableBaseModel):
    name: SourceColumnName
    type: SourceColumnTypeStr
    values: t.Tuple[t.Any | None, ...]

class SourceTable(ImmutableBaseModel):
    name: InstrumentName
    info: SourceTableInfo
    data: t.Mapping[SourceColumnName, SourceColumnData]