from __future__ import annotations
import typing as t
from pydantic import Field, validator

from ..common import ImmutableBaseModel

### CodeMapSpec

class CodeMapSpec(ImmutableBaseModel):
    class Value(t.TypedDict):
        value: int
        tag: str
        text: str

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

#class AggregateMeasureItem(ImmutableBaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

RelativeMeasureNodeName = t.NewType('RealtiveMeasureNodeName', str)
RelativeCodeMapName = t.NewType('RelativeCodeMapName', str)

class CodedMeasureItemSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'multiselect']
    codes: RelativeCodeMapName

class SimpleMeasureItemSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['text', 'real', 'integer']

class MeasureItemGroupSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNodeSpec, ...]

MeasureNodeSpec = t.Annotated[
    t.Union[
        MeasureItemGroupSpec,
        CodedMeasureItemSpec,
        SimpleMeasureItemSpec,
    ], Field(discriminator='type')
]

class MeasureSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeSpec, ...]
    codes: t.Mapping[RelativeCodeMapName, CodeMapSpec]

### Instrument

class QuestionInstrumentItemSpec(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: t.Optional[str]
    id: t.Optional[str]
    map: t.Optional[t.Mapping[str, t.Optional[str]]]

class ConstantInstrumentItemSpec(ImmutableBaseModel):
    type: t.Literal['constant']
    value: str
    id: t.Optional[str]

class InstrumentItemGroupSpec(ImmutableBaseModel):
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

class InstrumentSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNodeSpec, ...]

RelativeIndexColumnName = t.NewType('RelativeIndexColumnName', str)

class IndexColumnSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    values: CodeMapSpec

### Config

class StudyConfigSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    indices: t.Mapping[RelativeIndexColumnName, IndexColumnSpec]

class StudySpec(t.NamedTuple):
    config: StudyConfigSpec
    measures: t.Mapping[str, MeasureSpec]
    instruments: t.Mapping[str, InstrumentSpec]

MeasureItemGroupSpec.update_forward_refs()
CodeMapSpec.update_forward_refs()
InstrumentItemGroupSpec.update_forward_refs()