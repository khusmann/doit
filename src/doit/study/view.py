from __future__ import annotations
import typing as t

### ColumnView info to display columns

class CodemapValue(t.TypedDict):
    value: int
    tag: str
    text: str

CodemapRaw = t.Tuple[CodemapValue, ...]

class CodemapView(t.NamedTuple):
    tags: t.Mapping[int, str]
    labels: t.Mapping[int, str]

class CodedColumnView(t.NamedTuple):
    name: str
    studytable_name: t.Optional[str]
    prompt: str
    value_type: t.Literal['ordinal', 'categorical', 'multiselect']
    codes: CodemapView

class IndexColumnView(t.NamedTuple):
    name: str
    title: str
    description: t.Optional[str]
    value_type: t.Literal['index']
    codes: CodemapView

class SimpleColumnView(t.NamedTuple):
    name: str
    studytable_name: t.Optional[str]
    prompt: str
    type: t.Literal['text', 'integer', 'real']

ColumnView = t.Union[
    CodedColumnView,
    SimpleColumnView,
    IndexColumnView,
]

### InstrumentView - Info to populate an instrument's page

class QuestionInstrumentNodeView(t.NamedTuple):
    prompt: str
    source_column_name: t.Optional[str]
    column_info: t.Optional[ColumnView]
    map: t.Mapping[str, str]

class ConstantInstrumentNodeView(t.NamedTuple):
    constant_value: str
    column_info: t.Optional[ColumnView]

class GroupInstrumentNodeView(t.NamedTuple):
    title: t.Optional[str]
    prompt: t.Optional[str]
    items: t.Tuple[InstrumentNodeView, ...]

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

class IndexItemView(t.NamedTuple):
    name: str

class IndicesView(t.NamedTuple):
    items: t.Tuple[str, ...]

### MeasureView - Info to populate a measure's page

class CodedMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    value_type: t.Literal['ordinal', 'categorical', 'multiselect']
    codes: CodemapView

class SimpleMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    value_type: t.Literal['text', 'integer', 'real']

class GroupMeasureNodeView(t.NamedTuple):
    name: str
    prompt: str
    items: t.Tuple[MeasureNodeView, ...]

MeasureNodeView = t.Union[
    CodedMeasureNodeView,
    SimpleMeasureNodeView,
    GroupMeasureNodeView,
]

class MeasureView(t.NamedTuple):
    name: str
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNodeView, ...]

### Studytable View

class StudyTableView(t.NamedTuple):
    name: str
    columns: t.Tuple[ColumnView, ...]


### LinkerView

class QuestionSrcLink(t.NamedTuple):
    source_column_name: str
    source_value_map: t.Mapping[str, str]

class ConstantSrcLink(t.NamedTuple):
    constant_value: str

SrcLink = t.Union[
    QuestionSrcLink,
    ConstantSrcLink,
]

class CodedDstLink(t.NamedTuple):
    linked_name: str
    value_from_tag: t.Mapping[str, int]
    value_type: t.Literal['ordinal', 'categorical', 'multiselect', 'index', 'multiselect']

class SimpleDstLink(t.NamedTuple):
    linked_name: str
    value_type: t.Literal['text', 'real', 'integer']

DstLink = t.Union[
    CodedDstLink,
    SimpleDstLink,
]

class LinkerSpec(t.NamedTuple):
    src: SrcLink
    dst: DstLink

class InstrumentLinkerSpec(t.NamedTuple):
    studytable_name: str
    instrument_name: str
    linker_specs: t.Tuple[LinkerSpec, ...]