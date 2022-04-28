from __future__ import annotations
import typing as t

from ..common import ImmutableBaseModel

from ..common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

### ColumnView

class CodemapValue(ImmutableBaseModel):
    value: OrdinalValue
    tag: OrdinalTag
    text: OrdinalLabel

class CodemapRaw(ImmutableBaseModel):
    values: t.Tuple[CodemapValue, ...]

class CodemapView(ImmutableBaseModel):
    tags: t.Mapping[OrdinalValue, OrdinalTag]
    labels: t.Mapping[OrdinalValue, OrdinalLabel]

class OrdinalColumnView(ImmutableBaseModel):
    name: str
    studytable_name: t.Optional[str]
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
    studytable_name: t.Optional[str]
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
    constant_value: str
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


### LinkerView

class QuestionSrcLink(ImmutableBaseModel):
    source_column_name: str
    source_value_map: t.Mapping[str, str]

class ConstantSrcLink(ImmutableBaseModel):
    constant_value: str

SrcLink = t.Union[
    QuestionSrcLink,
    ConstantSrcLink,
]

class OrdinalDstLink(ImmutableBaseModel):
    linked_name: str
    value_from_tag: t.Mapping[str, int] 

class SimpleDstLink(ImmutableBaseModel):
    linked_name: str
    type: t.Literal['text', 'real', 'integer']

DstLink = t.Union[
    OrdinalDstLink,
    SimpleDstLink,
]

class LinkerSpec(t.NamedTuple):
    src: SrcLink
    dst: DstLink

GroupMeasureNodeView.update_forward_refs()
GroupInstrumentNodeView.update_forward_refs()