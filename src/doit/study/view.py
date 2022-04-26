from __future__ import annotations
import typing as t

from ..common.table import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

### InstrumentView - Info to populate an instrument's page

class QuestionInstrumentNodeView(t.NamedTuple):
    pass

class ConstantInstrumentNodeView(t.NamedTuple):
    pass

class GroupInstrumentNodeView(t.NamedTuple):
    pass

InstrumentNodeView = QuestionInstrumentNodeView | ConstantInstrumentNodeView | GroupInstrumentNodeView

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

class IndexItemView(t.NamedTuple):
    name: str

class IndicesView(t.NamedTuple):
    items: t.Tuple[str, ...]

### MeasureView - Info to populate a measure's page

class OrdinalMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]

class TextMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    # Sanitizer checksum

class GroupMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    items: t.Tuple[MeasureNodeView, ...]

MeasureNodeView = OrdinalMeasureNodeView | TextMeasureNodeView | GroupMeasureNodeView

class MeasureView(t.NamedTuple):
    name: str
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeView, ...]

### ColumnView

class OrdinalColumnView(t.NamedTuple):
    name: str
    prompt: str
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]

class TextColumnView(t.NamedTuple):
    name: str
    prompt: str
    # Sanitizer checksum

ColumnView = OrdinalColumnView | TextColumnView