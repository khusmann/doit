import typing as t
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
)
from pydantic.generics import GenericModel

class ImmutableBaseModel(BaseModel):
    class Config:
        allow_mutation=False

class ImmutableGenericModel(GenericModel):
    class Config:
        allow_mutation=False

T = t.TypeVar('T')
P = t.TypeVar('P')
def lift_none(func: t.Callable[[T], P]) -> t.Callable[[T | None], P | None]:
    def inner(i: T | None) -> P | None:
        return None if i is None else func(i)
    return inner

InstrumentId = t.NewType('InstrumentId', str)
ColumnId = t.NewType('ColumnId', str)

ColumnTypeStr = t.Literal['bool', 'ordinal', 'real', 'text', 'integer']
ColumnDataType = t.Union[StrictBool, StrictStr, StrictFloat, StrictInt]

RemoteService = t.Literal['qualtrics']
FormatType = t.Literal['qualtrics']

MeasureId = t.NewType("MeasureId", str)
MeasurePath = t.NewType("MeasurePath", str) # e.g. measure.group.item
MeasureItemId = t.NewType('MeasureItemId', str)
MeasureCodeMapId = t.NewType('MeasureCodeMapId', str)

StudyIdx = t.NewType('StudyIdx', str)
