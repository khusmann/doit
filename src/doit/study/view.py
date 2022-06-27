from __future__ import annotations
import typing as t

### ColumnView info to display columns

class CodemapValue(t.TypedDict):
    value: int
    tag: str
    text: str

CodemapRaw = t.Tuple[CodemapValue, ...]

class CodemapView(t.NamedTuple):
    tag_from_value: t.Mapping[int, str]
    label_from_value: t.Mapping[int, str]
    
    label_from_tag: t.Mapping[str, str]
    value_from_tag: t.Mapping[str, int]

    values: t.Tuple[CodemapValue, ...]

class CodedColumnView(t.NamedTuple):
    name: str
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
    prompt: str
    value_type: t.Literal['text', 'integer', 'real']

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
    map: t.Mapping[str, t.Optional[str]]
    type: t.Final = 'question'

class ConstantInstrumentNodeView(t.NamedTuple):
    constant_value: str
    column_info: t.Optional[ColumnView]
    type: t.Final = 'constant'

class GroupInstrumentNodeView(t.NamedTuple):
    title: t.Optional[str]
    prompt: t.Optional[str]
    items: t.Tuple[InstrumentNodeView, ...]
    type: t.Final = 'group'

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

### InstrumentListingView

class InstrumentListingItemView(t.NamedTuple):
    name: str
    title: str
    description: str
    indices: t.Tuple[str, ...]

class InstrumentListingView(t.NamedTuple):
    items: t.Tuple[InstrumentListingItemView, ...]

### MeasureListingView

class MeasureListingItemView(t.NamedTuple):
    name: str
    title: str
    description: str
    indices: t.Tuple[str, ...]

class MeasureListingView(t.NamedTuple):
    items: t.Tuple[MeasureListingItemView, ...]

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
    prompt: t.Optional[str]
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

### LinkerView

### ExcludeFilter

class MatchExcludeFilterSpec(t.NamedTuple):
    type: t.Literal['match']
    values: t.Mapping[str, str | None]

class CompareExcludeFilterSpec(t.NamedTuple):
    type: t.Literal['lt', 'gt', 'lte', 'gte']
    column: str
    value: str

ExcludeFilterSpec = MatchExcludeFilterSpec | CompareExcludeFilterSpec

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
    instrument_name: str
    exclude_filters: t.Tuple[ExcludeFilterSpec, ...]
    linker_specs: t.Tuple[LinkerSpec, ...]

### ColumnRawView

class ColumnRawView(t.NamedTuple):
    name: str
    table_name: str
    indices: t.Tuple[str, ...]