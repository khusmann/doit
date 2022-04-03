from __future__ import annotations
import typing as t
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
)
from pydantic.generics import GenericModel
from functools import reduce

class ImmutableBaseModel(BaseModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

class ImmutableBaseModelOrm(ImmutableBaseModel):
    class Config(ImmutableBaseModel.Config):
        orm_mode = True

class ImmutableGenericModel(GenericModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

T = t.TypeVar('T')
P = t.TypeVar('P')
Q = t.TypeVar('Q')

def lift_none(func: t.Callable[[T], P]) -> t.Callable[[T | None], P | None]:
    def inner(i: T | None) -> P | None:
        return None if i is None else func(i)
    return inner

def dmap(f: t.Callable[[P], Q], m: t.Mapping[T, P]) -> t.Mapping[T, Q]:
    return {
        k: f(v) for (k, v) in m.items()
    }

def merge_mappings(d: t.Sequence[t.Mapping[T, P]]) -> t.Dict[T, P]:
    return reduce(lambda acc, x: acc | x, map(lambda x: dict(x), d))

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

InstrumentId = t.NewType('InstrumentId', int)
InstrumentName = t.NewType('InstrumentName', str)
SourceColumnName = t.NewType('ColumnName', str)
InstrumentNodeId = t.NewType('InstrumentNodeId', int)

ColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
ColumnDataType = t.Union[StrictBool, StrictStr, StrictFloat, StrictInt]

RemoteServiceName = t.Literal['qualtrics']
FormatType = t.Literal['qualtrics']

MeasureId = t.NewType("MeasureId", int)
MeasureName = t.NewType("MeasureName", str)

RelativeMeasureNodeName = t.NewType('RelativeMeasureNodeName', str)
RelativeIndexColumnName = t.NewType('RelativeIndexName', str)

ColumnInfoId = t.NewType('ColumnInfoId', int)
class ColumnName(Uri): pass # e.g. measure.group.item

### CodeMap

CodeMapId = t.NewType('CodeMapId', int)
RelativeCodeMapName = t.NewType('RelativeCodeMapName', str)
class CodeMapName(Uri): pass
CodeValue = t.NewType('CodeValue', int)
CodeValueTag = t.NewType('CodeValueTag', str)
RecodeTransform = t.Mapping[CodeValueTag, t.Optional[CodeValueTag]]


def invert_map(m: t.Mapping[T, P]) -> t.Mapping[P, t.FrozenSet[T]]:
    result: t.Mapping[P, t.FrozenSet[T]] = {}
    for k, v in m.items():
        result[v] = result.get(v, frozenset()) | {k}
    return result

StudyTableId = t.NewType('TableId', int)
StudyTableName = t.NewType('TableName', str)