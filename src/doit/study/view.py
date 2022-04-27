from __future__ import annotations
import typing as t

from ..common import ImmutableBaseModel

from ..common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

### ColumnView

class CodemapView(ImmutableBaseModel):
    tags: t.Mapping[OrdinalValue, OrdinalTag]
    labels: t.Mapping[OrdinalValue, OrdinalLabel]

class OrdinalColumnView(ImmutableBaseModel):
    name: str
    prompt: str
    type: t.Literal['ordinal', 'categorical']
    codes: CodemapView

class IndexColumnView(ImmutableBaseModel):
    name: str
    title: str
    description: t.Optional[str]
    codes: CodemapView

class SimpleColumnView(ImmutableBaseModel):
    name: str
    prompt: str
    type: t.Literal['text', 'integer', 'real']

ColumnView = t.Union[
    OrdinalColumnView,
    SimpleColumnView,
    IndexColumnView,
]

### InstrumentView - Info to populate an instrument's page

class QuestionInstrumentNodeView(ImmutableBaseModel):
    prompt: str
    source_column_name: str
    column_info: t.Optional[ColumnView]
#    map: 

class ConstantInstrumentNodeView(ImmutableBaseModel):
    value: str
    column_info: t.Optional[ColumnView]

class GroupInstrumentNodeView(ImmutableBaseModel):
    title: t.Optional[str]
    prompt: t.Optional[str]
    items: t.Tuple[InstrumentNodeView, ...]

InstrumentNodeView = t.Union[
    QuestionInstrumentNodeView,
    ConstantInstrumentNodeView,
    GroupInstrumentNodeView,
]

class InstrumentView(ImmutableBaseModel):
    name: str
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]

    # data_checksum
    # schema_checksum
    # source_name

    nodes: t.Tuple[InstrumentNodeView, ...]

### IndicesView - Info to populate the indices page

class IndexItemView(ImmutableBaseModel):
    name: str

class IndicesView(ImmutableBaseModel):
    items: t.Tuple[str, ...]

### MeasureView - Info to populate a measure's page

class OrdinalMeasureNodeView(ImmutableBaseModel):
    name: str
    prompt: str
    type: t.Literal['ordinal', 'categorical']
    codes: CodemapView

class SimpleMeasureNodeView(ImmutableBaseModel):
    name: str
    prompt: str
    type: t.Literal['text', 'integer', 'real']

class GroupMeasureNodeView(ImmutableBaseModel):
    name: str
    prompt: str
    items: t.Tuple[MeasureNodeView, ...]

MeasureNodeView = t.Union[
    OrdinalMeasureNodeView,
    SimpleMeasureNodeView,
    GroupMeasureNodeView,
]

class MeasureView(ImmutableBaseModel):
    name: str
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeView, ...]

### Studytable View

class StudyTableView(ImmutableBaseModel):
    name: str
    columns: t.Tuple[ColumnView, ...]

GroupMeasureNodeView.update_forward_refs()
GroupInstrumentNodeView.update_forward_refs()