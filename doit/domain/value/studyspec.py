from __future__ import annotations
import typing as t
from .common import *
from pydantic import Field, validator

### CodeMapSpec

class CodeMapSpec(ImmutableBaseModel):
    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    __root__: t.Tuple[CodeMapSpec.Value, ...]

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

CodeMapSpec.update_forward_refs()

### Measure

#class AggregateMeasureItem(ImmutableBaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

class OrdinalMeasureItemSpec(ImmutableBaseModel):
    prompt: str
    type: t.Literal['ordinal', 'categorical']
    codes: RelativeCodeMapName

class SimpleMeasureItemSpec(ImmutableBaseModel):
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

MeasureItemSpec = t.Annotated[
    t.Union[
        OrdinalMeasureItemSpec,
        SimpleMeasureItemSpec,
    ], Field(discriminator='type')
]

class MeasureItemGroupSpec(ImmutableBaseModel):
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.OrderedDict[RelativeMeasureNodeName, MeasureNodeSpec]

MeasureNodeSpec = t.Annotated[
    t.Union[
        MeasureItemGroupSpec,
        MeasureItemSpec,
    ], Field(discriminator='type')
]

MeasureItemGroupSpec.update_forward_refs()

class MeasureSpec(ImmutableBaseModel):
    measure_id: MeasureName
    title: str
    description: t.Optional[str]
    items: t.OrderedDict[RelativeMeasureNodeName, MeasureNodeSpec]
    codes: t.Mapping[RelativeCodeMapName, CodeMapSpec]

### Instrument

class QuestionInstrumentItemSpec(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: SourceColumnName
    id: t.Optional[MeasureNodeName]
    map: t.Optional[RecodeTransform]

    # TODO: Custom export dict() rule to drop map if map is None

class ConstantInstrumentItemSpec(ImmutableBaseModel):
    type: t.Literal['constant']
    value: str
    id: MeasureNodeName

class HiddenInstrumentItemSpec(ImmutableBaseModel):
    type: t.Literal['hidden']
    remote_id: SourceColumnName
    id: MeasureNodeName
    map: t.Optional[RecodeTransform]

InstrumentItemSpec = t.Annotated[
    t.Union[
        QuestionInstrumentItemSpec,
        ConstantInstrumentItemSpec,
        HiddenInstrumentItemSpec,
    ], Field(discriminator='type')
]

class InstrumentItemGroupSpec(ImmutableBaseModel):
    type: t.Literal['group']
    items: t.Tuple[InstrumentNodeSpec, ...]
    prompt: str
    title: str

InstrumentNodeSpec = t.Annotated[
    t.Union[
        InstrumentItemSpec,
        InstrumentItemGroupSpec,
    ], Field(discriminator='type')
]

InstrumentItemGroupSpec.update_forward_refs()

class InstrumentSpec(ImmutableBaseModel):
    instrument_id: InstrumentName
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNodeSpec, ...]


class IndexColumnSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    values: CodeMapSpec

### Study
class ConfigSpec(ImmutableBaseModel):
    name: str
    description: t.Optional[str]
    indices: t.Mapping[IndexColumnName, IndexColumnSpec]

class StudySpec(ImmutableBaseModel):
    config: ConfigSpec
    measures: t.Mapping[MeasureName, MeasureSpec]
    instruments: t.Mapping[InstrumentName, InstrumentSpec]

### Table
class TableSpec(ImmutableBaseModel):
    indices: t.FrozenSet[MeasureNodeName]
    columns: t.FrozenSet[MeasureNodeName]
    @property
    def tag(self):
        return '-'.join(sorted(self.indices))