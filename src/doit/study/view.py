from __future__ import annotations
import typing as t
from pydantic import BaseModel

from ..common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

### InstrumentView - Info to populate an instrument's page

class QuestionInstrumentNodeView(BaseModel):
    pass

class ConstantInstrumentNodeView(BaseModel):
    pass

class GroupInstrumentNodeView(BaseModel):
    pass

InstrumentNodeView = t.Union[
    QuestionInstrumentNodeView,
    ConstantInstrumentNodeView,
    GroupInstrumentNodeView,
]

class InstrumentView(t.NamedTuple):
    name: str
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]

    # data_checksum
    # schema_checksum
    # source_name

    nodes: t.Tuple[InstrumentNodeView, ...]

### IndicesView - Info to populate the indices page

class IndexItemView(BaseModel):
    name: str

class IndicesView(BaseModel):
    items: t.Tuple[str, ...]

### MeasureView - Info to populate a measure's page

class OrdinalMeasureNodeView(BaseModel):
    name: str
    prompt: str
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]
    type: t.Literal['ordinal', 'categorical']
    entity_type: t.Literal['ordinalmeasurenode']

class SimpleMeasureNodeView(BaseModel):
    name: str
    prompt: str
    type: t.Literal['text', 'integer', 'real']
    entity_type: t.Literal['simplemeasurenode']

class GroupMeasureNodeView(BaseModel):
    name: str
    prompt: str
    items: t.Tuple[MeasureNodeView, ...]
    entity_type: t.Literal['groupmeasurenode']

MeasureNodeView = t.Union[
    OrdinalMeasureNodeView,
    SimpleMeasureNodeView,
    GroupMeasureNodeView,
]

class MeasureView(BaseModel):
    name: str
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeView, ...]

### ColumnView

class OrdinalColumnView(BaseModel):
    name: str
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'index']
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]
    entity_type: t.Literal['ordinalcolumn']

class SimpleColumnView(BaseModel):
    name: str
    prompt: str
    type: t.Literal['text', 'integer', 'real']
    entity_type: t.Literal['simplecolumn']

ColumnView = t.Union[
    OrdinalColumnView,
    SimpleColumnView,
]

GroupMeasureNodeView.update_forward_refs()