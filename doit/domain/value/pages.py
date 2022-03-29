from __future__ import annotations
import typing as t
from .common import *
from pydantic import Field

### Measures

class CodeMap(ImmutableBaseModelOrm):
    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    values: t.Tuple[CodeMap.Value, ...]

    def tag_to_value(self):
        return { pair['tag']: pair['value'] for pair in self.values }

    def value_to_tag(self):
        return { pair['value']: pair['tag'] for pair in self.values }



class MeasureListing(ImmutableBaseModelOrm):
    tag: str
    title: str
    description: t.Optional[str]

class OrdinalMeasureItem(ImmutableBaseModelOrm):
    tag: str
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']
    codes: CodeMap

class SimpleMeasureItem(ImmutableBaseModelOrm):
    tag: str
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

MeasureItem = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
    ], Field(discriminator='type')
]

class MeasureItemGroup(ImmutableBaseModelOrm):
    tag: str
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNode, ...]

MeasureNode = t.Annotated[
    t.Union[
        MeasureItemGroup,
        MeasureItem,
    ], Field(discriminator='type')
]

MeasureItemGroup.update_forward_refs()

class Measure(ImmutableBaseModelOrm):
    tag: str
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNode, ...]

OrdinalMeasureItem.update_forward_refs()

### Instruments

class InstrumentListing(ImmutableBaseModelOrm):
    tag: MeasureId
    title: str
    description: t.Optional[str]

class QuestionInstrumentItem(ImmutableBaseModelOrm):
    prompt: str
    type: t.Literal['question']
    measure_item: t.Optional[MeasureItem]
    #id: t.Optional[MeasureItemId]
    #map: t.Optional[RecodeTransform]

    # TODO: Custom export dict() rule to drop map if map is None

class ConstantInstrumentItem(ImmutableBaseModelOrm):
    type: t.Literal['constant']
    value: t.Any
    #id: MeasureItemId

class HiddenInstrumentItem(ImmutableBaseModelOrm):
    type: t.Literal['hidden']
    remote_id: ColumnId
    #id: MeasureItemId

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(ImmutableBaseModelOrm):
    type: t.Literal['group']
    items: t.Tuple[InstrumentNode, ...]
    prompt: str
    title: str

InstrumentNode = t.Annotated[
    t.Union[
        InstrumentItem,
        InstrumentItemGroup,
    ], Field(discriminator='type')
]

InstrumentItemGroup.update_forward_refs()

class Instrument(ImmutableBaseModelOrm):
    tag: MeasureId
    title: str
    description: t.Optional[str]
    items: t.Tuple[InstrumentNode, ...]