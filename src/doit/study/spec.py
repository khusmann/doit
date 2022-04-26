from __future__ import annotations
import typing as t
from pydantic import Field, validator, BaseModel

from ..common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

### CodeMapSpec

class CodeMapSpec(BaseModel):
    class Value(t.TypedDict):
        value: OrdinalValue
        tag: OrdinalTag
        text: OrdinalLabel

    __root__: t.Tuple[CodeMapSpec.Value, ...]

    @validator('__root__', pre=True)
    def dict_or_set(cls, input: t.Sequence[t.Any]) -> t.Any:
        assert(len(input) > 0)
        if isinstance(input[0], dict):
            return input
        else:
            return [ {'value': i, 'tag': v, 'text': v} for (i, v) in enumerate(input) ]

    ## TODO: Validate uniqueness of pair.*

### Measure

#class AggregateMeasureItem(BaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

RelativeMeasureNodeName = t.NewType('RealtiveMeasureNodeName', str)
RelativeCodeMapName = t.NewType('RelativeCodeMapName', str)

class OrdinalMeasureItemSpec(BaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['ordinal', 'categorical']
    codes: RelativeCodeMapName

class SimpleMeasureItemSpec(BaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['text', 'real', 'integer']

class MeasureItemGroupSpec(BaseModel):
    id: RelativeMeasureNodeName
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNodeSpec, ...]

class MultiselectItemSpec(BaseModel):
    id: RelativeMeasureNodeName
    prompt: t.Optional[str]
    type: t.Literal['multiselect']
    items: t.Tuple[MultiselectItemSpec.Child, ...]

    class Child(BaseModel):
        id: RelativeMeasureNodeName
        prompt: str
        type: t.Literal['bool'] = 'bool'

MeasureNodeSpec = t.Annotated[
    t.Union[
        MeasureItemGroupSpec,
        OrdinalMeasureItemSpec,
        SimpleMeasureItemSpec,
        MultiselectItemSpec,
    ], Field(discriminator='type')
]

class MeasureSpec(BaseModel):
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeSpec, ...]
    codes: t.Mapping[RelativeCodeMapName, CodeMapSpec]

### Instrument

class QuestionInstrumentItemSpec(BaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: t.Optional[str]
    id: t.Optional[str]
    map: t.Optional[t.Mapping[str, t.Optional[str]]]

class ConstantInstrumentItemSpec(BaseModel):
    type: t.Literal['constant']
    value: str
    id: str

class InstrumentItemGroupSpec(BaseModel):
    type: t.Literal['group']
    items: t.Tuple[InstrumentNodeSpec, ...]
    prompt: t.Optional[str]
    title: t.Optional[str]

InstrumentNodeSpec = t.Annotated[
    t.Union[
        QuestionInstrumentItemSpec,
        ConstantInstrumentItemSpec,
        InstrumentItemGroupSpec,
    ], Field(discriminator='type')
]

class InstrumentSpec(BaseModel):
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNodeSpec, ...]

RelativeIndexColumnName = t.NewType('RelativeIndexColumnName', str)

class IndexColumnSpec(BaseModel):
    title: str
    description: t.Optional[str]
    values: CodeMapSpec

### Config

class StudyConfigSpec(BaseModel):
    title: str
    description: t.Optional[str]
    indices: t.Mapping[RelativeIndexColumnName, IndexColumnSpec]

class StudySpec(t.NamedTuple):
    config: StudyConfigSpec
    measures: t.Mapping[str, MeasureSpec]
    instruments: t.Mapping[str, InstrumentSpec]

    

MeasureItemGroupSpec.update_forward_refs()
MultiselectItemSpec.update_forward_refs()
CodeMapSpec.update_forward_refs()
InstrumentItemGroupSpec.update_forward_refs()