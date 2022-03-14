from __future__ import annotations
from dataclasses import dataclass
import typing as t
from .common import *
from pydantic import Field

### Measure

#class AggregateMeasureItem(ImmutableBaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

class OrdinalMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']
    codes: CodeMapTag
    is_idx: t.Optional[bool] = False

class SimpleMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

MeasureItem = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
    ], Field(discriminator='type')
]

class MeasureItemGroup(ImmutableBaseModel):
    prompt: str
    type: t.Literal['group']
    items: t.OrderedDict[MeasureNodeTag, MeasureNode]

MeasureNode = t.Annotated[
    t.Union[
        MeasureItemGroup,
        MeasureItem,
    ], Field(discriminator='type')
]

MeasureItemGroup.update_forward_refs()

class Measure(ImmutableBaseModel):
    measure_id: MeasureId
    title: str
    description: t.Optional[str]
    items: t.OrderedDict[MeasureNodeTag, MeasureNode]
    codes: t.Mapping[CodeMapTag, CodeMap]

### Instrument

class QuestionInstrumentItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: ColumnId
    measure_id: t.Optional[MeasureItemUri]
    map: t.Optional[RecodeTransform]

    # TODO: Custom export dict() rule to drop map if map is None

class ConstantInstrumentItem(ImmutableBaseModel):
    type: t.Literal['constant']
    value: t.Any
    measure_id: MeasureItemUri

class HiddenInstrumentItem(ImmutableBaseModel):
    type: t.Literal['hidden']
    remote_id: ColumnId
    measure_id: MeasureItemUri

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(ImmutableBaseModel):
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

class Instrument(ImmutableBaseModel):
    instrument_id: InstrumentId
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentNode, ...]

### Study

@dataclass(frozen=True)
class Study:
    title: str
    description: t.Optional[str]
    measures: t.Mapping[MeasureId, Measure]
    instruments: t.Mapping[InstrumentId, Instrument]
    #source_meta: t.Mapping[InstrumentId, SourceMeta]

    # Derived properties
    measure_items: t.Mapping[MeasureItemUri, MeasureItem]
    codemaps: t.Mapping[CodeMapUri, CodeMap]
    tables: t.Mapping[StudyTableId, t.FrozenSet[MeasureItemUri]]