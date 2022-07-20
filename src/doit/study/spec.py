from __future__ import annotations
import typing as t
from pydantic import Field, validator
import re

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
            return [ {'value': int(v), 'tag': v, 'text': v} for v in input ]

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

class CompositeItemSpec(ImmutableBaseModel):
    name: RelativeMeasureNodeName
    reverse_coded: bool

def composite_item_spec_from_str(value: str):
    reverse_coded = re.search(r'^rev\((.*)\)', value)
    if reverse_coded:
        return CompositeItemSpec(
            name=reverse_coded.group(1),
            reverse_coded=True,
        )
    return CompositeItemSpec(
        name=value,
        reverse_coded=False,
    )

class MeasureCompositeMeanSpec(ImmutableBaseModel):
    id: RelativeMeasureNodeName
    title: str
    type: t.Literal['mean']
    items: t.Tuple[CompositeItemSpec, ...]

    @validator('items', pre=True)
    def item_spec(cls, values: t.Tuple[str]):
        return tuple(composite_item_spec_from_str(v) for v in values)

MeasureCompositeSpec = MeasureCompositeMeanSpec

class MeasureSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    indices: t.Optional[t.Tuple[str, ...]]
    items: t.Tuple[MeasureNodeSpec, ...]
    codes: t.Mapping[RelativeCodeMapName, CodeMapSpec] = {}
    composites: t.Tuple[MeasureCompositeSpec, ...] = ()

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
    title: t.Optional[str]
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[InstrumentNodeSpec, ...]

InstrumentNodeSpec = t.Annotated[
    t.Union[
        QuestionInstrumentItemSpec,
        ConstantInstrumentItemSpec,
        InstrumentItemGroupSpec,
    ], Field(discriminator='type')
]

### ExcludeFilter

class MatchExcludeFilter(ImmutableBaseModel):
    type: t.Literal['match']
    values: t.Mapping[str, t.Optional[str]]

class CompareExcludeFilter(ImmutableBaseModel):
    type: t.Literal['lt', 'gt', 'lte', 'gte']
    column: str
    value: str

ExcludeFilter = t.Annotated[
    t.Union[
        MatchExcludeFilter,
        CompareExcludeFilter,
    ], Field(discriminator='type')
]

class InstrumentSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNodeSpec, ...]
    exclude_filters: t.Tuple[ExcludeFilter, ...] = ()

RelativeIndexColumnName = t.NewType('RelativeIndexColumnName', str)

class IndexColumnSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    values: CodeMapSpec

### Package

class PivotTransformSpec(ImmutableBaseModel):
    type: t.Literal['pivot']
    index: str


class RenameTransformSpec(ImmutableBaseModel):
    type: t.Literal['rename']
    map: t.Mapping[str, str]

PackageTransformSpec = t.Union[
    PivotTransformSpec,
    RenameTransformSpec
]

class PackageSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    output: str
    items: t.Tuple[str, ...]
    transforms: t.Tuple[PackageTransformSpec, ...]

### Config

class StudyConfigSpec(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    indices: t.Mapping[RelativeIndexColumnName, IndexColumnSpec]

class StudySpec(t.NamedTuple):
    config: StudyConfigSpec
    measures: t.Mapping[str, MeasureSpec]
    instruments: t.Mapping[str, InstrumentSpec]
    packages: t.Mapping[str, PackageSpec]

MeasureItemGroupSpec.update_forward_refs()
CodeMapSpec.update_forward_refs()
InstrumentItemGroupSpec.update_forward_refs()