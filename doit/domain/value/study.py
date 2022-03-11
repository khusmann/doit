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
    is_idx: t.Optional[bool] = False

class SimpleMeasureItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

ValueMeasureItem = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
    ], Field(discriminator='type')
]

def is_measure_item_idx(item: ValueMeasureItem) -> bool:
    return item.type == 'ordinal' and item.is_idx is not None and item.is_idx

MeasureItem = t.Annotated[
    t.Union[
        GroupMeasureItem,
        ValueMeasureItem,
    ], Field(discriminator='type')
]

GroupMeasureItem.update_forward_refs()

class CodeMapItem(ImmutableBaseModel):
    value: int
    text: str

CodeMap = t.Mapping[str, CodeMapItem] | t.Set[str]

class Measure(ImmutableBaseModel):
    measure_id: MeasureId
    title: str
    description: t.Optional[str]
    items: t.OrderedDict[MeasureItemId, MeasureItem]
    codes: t.OrderedDict[MeasureCodeMapId, CodeMap]

### Instrument

QuestionValueMapper = t.Mapping[str, t.Optional[str]]

class QuestionInstrumentItem(ImmutableBaseModel):
    prompt: str
    type: t.Literal['question']
    remote_id: ColumnId
    measure_id: t.Optional[MeasurePath]
    map: t.Optional[QuestionValueMapper]

    # TODO: Custom export dict() rule to drop map if map is None

class GroupInstrumentItem(ImmutableBaseModel):
    type: t.Literal['group']
    items: t.Tuple[InstrumentItem, ...]
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

ValueInstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

InstrumentItem = t.Annotated[
    t.Union[
        ValueInstrumentItem,
        GroupInstrumentItem,
    ], Field(discriminator='type')
]

GroupInstrumentItem.update_forward_refs()

class Instrument(ImmutableBaseModel):
    instrument_id: InstrumentId
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]
    items: t.Tuple[InstrumentItem, ...]

    def valueitems_flat(self) -> t.Sequence[ValueInstrumentItem]:
        def trav(items: t.Sequence[InstrumentItem]) -> t.Sequence[ValueInstrumentItem]:
            return sum([ trav(i.items) if i.type == 'group' else [i] for i in items], [])
        return trav(self.items)

### Study

class Study(ImmutableBaseModel):
    title: str
    description: t.Optional[str]
    measures: t.OrderedDict[MeasureId, Measure]
    instruments: t.OrderedDict[InstrumentId, Instrument]

    def resolve_measure_path(self, path_id: MeasurePath) -> ValueMeasureItem:
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
        result = resolve(self.measures[measure_id].items[item_id], [MeasureItemId(i) for i in parts[2:]])

        assert result.type != 'group'
        return result

### LinkedTableSpec
class LinkedColumnSpec(ImmutableBaseModel):
    measure_item: ValueMeasureItem
    instrument_item: ValueInstrumentItem

class LinkedTableSpec(ImmutableBaseModel):
    instrument_id: InstrumentId
    columns: t.Mapping[MeasurePath, LinkedColumnSpec]
    @property
    def indices(self) -> t.Set[MeasurePath]:
        return {
            measure_id for (measure_id, column) in self.columns.items() if is_measure_item_idx(column.measure_item)
        }