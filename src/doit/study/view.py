from __future__ import annotations
import typing as t
from abc import ABC

from ..common import (
    OrdinalLabel,
    OrdinalTag,
    OrdinalValue,
)

MeasureName = t.NewType('MeasureName', str)
IndexName = t.NewType('IndexName', str)
MeasureNodeName = t.NewType('MeasureNodeName', str)
InstrumentName = t.NewType('InstrumentName', str)
ColumnName = t.NewType('ColumnName', str)

### StudyRepo Interface

class StudyRepoWriter(ABC):
    def query_linker(self, instrument_name: str): ... # TODO return type Linker
    # In service: link_table(table: SanitizedTable, linker: Linker) -> LinkedTable
    def write_table(self, table: str): ... # TODO Change to type LinkedTable

class StudyRepoReader(ABC):
    def query_instrument(self, instrument_name: str) -> InstrumentView: ...
    def query_measure(self, measure_name: str) -> MeasureView: ...
    def query_column(self, column_name: str) -> ColumnView: ...
    # def query_table(self, columns: t.Sequence[str]) -> SubsetView: ...

### InstrumentView - Info to populate an instrument's page

class QuestionInstrumentNodeView(t.NamedTuple):
    pass

class ConstantInstrumentNodeView(t.NamedTuple):
    pass

class GroupInstrumentNodeView(t.NamedTuple):
    pass

InstrumentNodeView = QuestionInstrumentNodeView | ConstantInstrumentNodeView | GroupInstrumentNodeView

class InstrumentView(t.NamedTuple):
    name: InstrumentName
    title: str
    description: t.Optional[str]
    instructions: t.Optional[str]

    # data_checksum
    # schema_checksum
    # source_name

    nodes: t.Tuple[InstrumentNodeView, ...]

### IndicesView - Info to populate the indices page

class IndexItemView(t.NamedTuple):
    name: IndexName

class IndicesView(t.NamedTuple):
    items: t.Tuple[IndexName, ...]

### MeasureView - Info to populate a measure's page

class OrdinalMeasureNodeView(t.NamedTuple):
    name: MeasureNodeName
    prompt: str
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]

class TextMeasureNodeView(t.NamedTuple):
    name: MeasureNodeName
    prompt: str
    # Sanitizer checksum

class GroupMeasureNodeView(t.NamedTuple):
    name: MeasureNodeName
    prompt: str
    items: MeasureNodeView

MeasureNodeView = OrdinalMeasureNodeView | TextMeasureNodeView

class MeasureView(t.NamedTuple):
    name: MeasureName
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeView, ...]

### ColumnView

class OrdinalColumnView(t.NamedTuple):
    name: ColumnName
    prompt: str
    tag_map: t.Mapping[OrdinalValue, OrdinalTag]
    label_map: t.Mapping[OrdinalValue, OrdinalLabel]

class TextColumnView(t.NamedTuple):
    name: ColumnName
    prompt: str
    # Sanitizer checksum

ColumnView = OrdinalColumnView | TextColumnView