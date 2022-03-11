from __future__ import annotations
import typing as t
from .common import *
from pydantic import Field

### Measure

class GroupMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['group']
    items: t.OrderedDict[MeasureItemId, MeasureItem]

#class AggregateMeasureItem(ImmutableBaseModel):
#    prompt: str
#    type: t.Literal['aggregate']
#    aggretate_type: t.Literal['mean']
#    items: t.Tuple[Measure.ItemId]

class OrdinalMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']
    codes: MeasureCodeMapId

class ValueMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

MeasureItem = t.Annotated[
    t.Union[
        GroupMeasureItem,
        OrdinalMeasureItem,
        ValueMeasureItem,
    ], Field(discriminator='type')
]

GroupMeasureItem.update_forward_refs()

class CodeMapValue(ImmutableBaseModel):
    value: int
    text: str

CodeMap = t.Mapping[str, CodeMapValue]

class Measure(ImmutableBaseModel):
    measure_id: MeasureId
    title: str
    description: t.Optional[str]
    items: t.OrderedDict[MeasureItemId, MeasureItem]
    codes: t.OrderedDict[MeasureCodeMapId, CodeMap]

### Instrument

class QuestionNoMapInstrumentItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: ColumnId
    measure_id: t.Optional[MeasurePath]

class QuestionMapInstrumentItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: ColumnId
    measure_id: t.Optional[MeasurePath]
    map: t.Mapping[str, t.Optional[str]]

QuestionInstrumentItem = t.Annotated[
    t.Union[
        QuestionMapInstrumentItem, # NOTE: Must come first in the union
        QuestionNoMapInstrumentItem,
    ], Field(discriminator='map')
]

class GroupInstrumentItem(ImmutableBaseModel):
    type: t.Literal['group']
    items: t.Tuple[InstrumentItem]
    prompt: str
    title: str

class ConstantInstrumentItem(ImmutableBaseModel):
    type: t.Literal['constant']
    value: t.Any
    measure_id: MeasurePath

class HiddenInstrumentItem(ImmutableBaseModel):
    type: t.Literal['hidden']
    remote_id: ColumnId
    measure_id: MeasurePath

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        GroupInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

GroupInstrumentItem.update_forward_refs()

class Instrument(ImmutableBaseModel):
    instrument_id: InstrumentId
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentItem, ...]


### Study

class Study(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    measures: t.OrderedDict[MeasureId, Measure]
    instruments: t.OrderedDict[InstrumentId, Instrument]

    def resolve_measure_path(self, path_id: MeasurePath) -> MeasureItem:
        def resolve(cursor: MeasureItem, remainder: t.Sequence[MeasureItemId]) -> MeasureItem:
            if len(remainder) > 0:
                assert cursor.type == 'group'
                return resolve(cursor.items[remainder[0]], remainder[1:])
            else:
                return cursor
        
        parts = path_id.split('.')
        assert len(parts) >= 2

        measure_id = MeasureId(parts[0])
        item_id = MeasureItemId(parts[1])
        return resolve(self.measures[measure_id].items[item_id], [MeasureItemId(i) for i in parts[2:]])
