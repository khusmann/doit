from __future__ import annotations
import typing as t
from pydantic import BaseModel
from pydantic.generics import GenericModel

class ImmutableBaseModel(BaseModel):
    class Config:
        frozen=True
        smart_union = True

class ImmutableGenericModel(GenericModel):
    class Config:
        frozen=True
        smart_union = True

class ImmutableBaseModelOrm(ImmutableBaseModel):
    class Config(ImmutableBaseModel.Config):
        orm_mode = True

class Uri(str):
    def as_tuple(self) -> t.Tuple[str, ...]:
        return tuple(self.split('.'))

    @classmethod
    def from_tuple(cls, v: t.Tuple[str, ...]):
        return cls('.'.join(v))

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: t.Any):
        if not isinstance(v, str):
            raise TypeError('string required')
        return cls(v)

    def __truediv__(self, v: str | Uri):
        match v:
            case Uri():
                return self.from_tuple((*self.as_tuple(), *v.as_tuple()))
            case str():
                return self.from_tuple((*self.as_tuple(), v))

# TODO: Put a base codemap here as well?

### Source Table Types (TODO: Clean these up)

SourceColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
SourceColumnName = t.NewType('SourceColumnName', str)

RemoteServiceName = t.NewType('RemoveServiceName', str)
SourceFormatType = t.Literal['qualtrics']

### Source Table Entity Ids
SourceTableInfoId = t.NewType("SourceTableId", int)
SourceColumnInfoId = t.NewType("SourceColumnInfoId", int)

### StudyTable Types

OrdinalStudyColumnTypeStr = t.Literal['ordinal', 'categorical', 'index']
SimpleStudyColumnTypeStr = t.Literal['text', 'real', 'integer', 'bool']

StudyColumnTypeStr = t.Union[
    OrdinalStudyColumnTypeStr,
    SimpleStudyColumnTypeStr,
]

### Relative Names (used by specs)

RelativeMeasureNodeName = t.NewType('RelativeMeasureNodeName', str)
RelativeIndexColumnName = t.NewType('RelativeIndexName', str)
RelativeCodeMapName = t.NewType('RelativeCodeMapName', str)

### CodeMap Values

CodeValue = t.NewType('CodeValue', int)
CodeValueTag = t.NewType('CodeValueTag', str)
RecodeTransform = t.Mapping[str, t.Optional[str]]

### Study Entity Ids

CodeMapId = t.NewType('CodeMapId', int)
MeasureId = t.NewType("MeasureId", int)
ColumnInfoNodeId = t.NewType('ColumnInfoId', int)
InstrumentId = t.NewType('InstrumentId', int)
InstrumentNodeId = t.NewType('InstrumentNodeId', int)
StudyTableId = t.NewType('TableId', int)

StudyEntityId = t.Union[
    CodeMapId,
    MeasureId,
    ColumnInfoNodeId,
    InstrumentId,
    InstrumentNodeId,
    StudyTableId,
]

### Entity Names

MeasureName = t.NewType("MeasureName", str)
StudyTableName = t.NewType('TableName', str)
InstrumentName = t.NewType('InstrumentName', str)
class CodeMapName(Uri): pass
class ColumnName(Uri): pass # e.g. measure.group.item

StudyEntityName = t.Union[
    MeasureName,
    InstrumentName,
    StudyTableName,
    CodeMapName,
    ColumnName,
]

