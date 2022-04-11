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

    ## TODO: Validate uniqueness of pair.*

### Measure

#class AggregateMeasureItem(ImmutableBaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

class OrdinalMeasureItemSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['ordinal', 'categorical']
    codes: RelativeCodeMapName

class SimpleMeasureItemSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

class MeasureItemGroupSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNodeSpec, ...]

MeasureNodeSpec = t.Annotated[
    t.Union[
        MeasureItemGroupSpec,
        OrdinalMeasureItemSpec,
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
    remote_id: SourceColumnName
    id: t.Optional[ColumnName]
    map: t.Optional[RecodeTransform]

class ConstantInstrumentItemSpec(ImmutableBaseModel):
    type: t.Literal['constant']
    value: str
    id: ColumnName

InstrumentItemSpec = t.Annotated[
    t.Union[
        QuestionInstrumentItemSpec,
        ConstantInstrumentItemSpec,
    ], Field(discriminator='type')
]

class InstrumentItemGroupSpec(ImmutableBaseModel):
    type: t.Literal['group']
    items: t.Tuple[InstrumentNodeSpec, ...]
    prompt: t.Optional[str]
    title: t.Optional[str]

InstrumentNodeSpec = t.Annotated[
    t.Union[
        InstrumentItemSpec,
        InstrumentItemGroupSpec,
    ], Field(discriminator='type')
]

class InstrumentSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNodeSpec, ...]

    def flat_items(self):
        def impl(nodes: t.Tuple[InstrumentNodeSpec, ...]) -> t.Generator[InstrumentItemSpec, None, None]:
            for n in nodes:
                if n.type == 'group':
                    yield from impl(n.items)
                else:
                    yield n
        return impl(self.items)

    def index_column_names(self):
        return (
            RelativeIndexColumnName(i.id.removeprefix("indices.")) 
                for i in self.flat_items()
                    if (i.id is not None) and i.id.startswith('indices.')
        )

class IndexColumnSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    values: CodeMapSpec

### Config
class StudyConfigSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    indices: t.Mapping[RelativeIndexColumnName, IndexColumnSpec]

class StudySpec(ImmutableBaseModel):
    config: StudyConfigSpec
    measures: t.Mapping[MeasureName, MeasureSpec]
    instruments: t.Mapping[InstrumentName, InstrumentSpec]

MeasureItemGroupSpec.update_forward_refs()
CodeMapSpec.update_forward_refs()
InstrumentItemGroupSpec.update_forward_refs()