from __future__ import annotations
import typing as t
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
    validator,
)
from pydantic.generics import GenericModel

class ImmutableBaseModel(BaseModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

class ImmutableGenericModel(GenericModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

T = t.TypeVar('T')
P = t.TypeVar('P')
def lift_none(func: t.Callable[[T], P]) -> t.Callable[[T | None], P | None]:
    def inner(i: T | None) -> P | None:
        return None if i is None else func(i)
    return inner

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


InstrumentId = t.NewType('InstrumentId', str)
ColumnId = t.NewType('ColumnId', str)

ColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
ColumnDataType = t.Union[StrictBool, StrictStr, StrictFloat, StrictInt]

RemoteService = t.Literal['qualtrics']
FormatType = t.Literal['qualtrics']

MeasureId = t.NewType("MeasureId", str)
class MeasureItemUri(Uri): pass # e.g. measure.group.item
MeasureNodeTag = t.NewType('MeasureNodeTag', str)


### CodeMap

CodeMapTag = t.NewType('CodeMapTag', str)
class CodeMapUri(Uri): pass
CodeValue = t.NewType('CodeValue', int)
CodeValueTag = t.NewType('CodeValueTag', str)
RecodeTransform = t.Mapping[CodeValueTag, t.Optional[CodeValueTag]]

class CodeMap(ImmutableBaseModel):
    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    __root__: t.Tuple[CodeMap.Value, ...]

    @validator('__root__', pre=True)
    def dict_or_set(cls, input: t.Sequence[t.Any]) -> t.Any:
        assert(len(input) > 0)
        if isinstance(input[0], dict):
            return input
        else:
            return [ {'value': i, 'tag': v, 'text': v} for (i, v) in enumerate(input) ]

    def tag_to_value(self):
        return { pair['tag']: pair['value'] for pair in self.__root__ }

    def value_to_tag(self):
        return { pair['value']: pair['tag'] for pair in self.__root__ }

    ## TODO: Validate uniqueness of pair.*

CodeMap.update_forward_refs()

def invert_map(m: t.Mapping[T, P]) -> t.Mapping[P, t.FrozenSet[T]]:
    result: t.Mapping[P, t.FrozenSet[T]] = {}
    for k, v in m.items():
        result[v] = result.get(v, frozenset()) | {k}
    return result

StudyTableId = t.NewType('StudyTableId', t.FrozenSet[MeasureItemUri])