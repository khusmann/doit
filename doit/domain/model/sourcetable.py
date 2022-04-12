import typing as t

from ..value import *

class SourceColumnInfo(ImmutableBaseModel):
    name: SourceColumnName
    type: SourceColumnTypeStr
    prompt: str
    sanitizer_info: t.Optional[str] # TODO
    # Codemap?

class SourceColumnEntry(ImmutableBaseModelOrm):
    id: SourceColumnEntryId
    parent_table_id: SourceTableEntryId
    content: SourceColumnInfo

class SourceTableEntry(ImmutableBaseModelOrm):
    id: SourceTableEntryId
    name: InstrumentName
    content: SourceTableInfo
    columns: t.Dict[SourceColumnName, SourceColumnEntry]

class SourceColumn(ImmutableBaseModel):
    entry: SourceColumnEntry
    values: t.Tuple[t.Any | None, ...]

class SourceTable(ImmutableBaseModel):
    entry: SourceTableEntry
    columns: t.Mapping[SourceColumnName, SourceColumn]