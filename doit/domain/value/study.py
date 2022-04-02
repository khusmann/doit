from __future__ import annotations
import typing as t

from .common import *
from pydantic import Field
from abc import ABC

from .studyspec import *

class StudyEntity(ImmutableBaseModel, ABC):
    class Config(ImmutableBaseModel.Config):
        orm_mode = True

### CodeMap

class CodeMap(StudyEntity):
    id: CodeMapId
    name: CodeMapName

    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    values: t.Tuple[CodeMap.Value, ...]

    def tag_to_value(self):
        return { pair['tag']: pair['value'] for pair in self.values }

    def value_to_tag(self):
        return { pair['value']: pair['tag'] for pair in self.values }

CodeMap.update_forward_refs()

### StudyTable

class StudyTable(StudyEntity):
    id: StudyTableId
    name: StudyTableName
    index_items: t.Tuple[IndexColumn, ...]
    measure_items: t.Tuple[MeasureItem, ...]

### IndexItem

class IndexColumn(StudyEntity):
    id: IndexColumnId
    name: IndexColumnName
    codemap_id: CodeMapId

### Measures
class OrdinalMeasureItem(StudyEntity):
    id: MeasureNodeId
    name: MeasureNodeName
    parent_node_id: t.Optional[MeasureNodeId]
    parent_measure_id: t.Optional[MeasureId]
    studytable_id: t.Optional[StudyTableId]
    codemap_id: CodeMapId
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']

    @classmethod
    def from_spec(cls, id: int, rel_name: RelativeMeasureNodeName, spec: OrdinalMeasureItemSpec, parent: MeasureNode | Measure, codemap_lookup: t.Mapping[RelativeCodeMapName, CodeMapId]):
        parent_node_id=parent.id if not isinstance(parent, Measure) else None
        parent_measure_id=parent.id if isinstance(parent, Measure) else None
        return cls(
            id=id,
            name=MeasureNodeName(parent.name) / rel_name,
            parent_node_id=parent_node_id,
            parent_measure_id=parent_measure_id,
            studytable_id=None,
            codemap_id=codemap_lookup[spec.codes],
            prompt=spec.prompt,
            type=spec.type,
        )

class SimpleMeasureItem(StudyEntity):
    id: MeasureNodeId
    name: MeasureNodeName
    parent_node_id: t.Optional[MeasureNodeId]
    parent_measure_id: t.Optional[MeasureId]
    studytable_id: t.Optional[StudyTableId]
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

    @classmethod
    def from_spec(cls, id: int, rel_name: RelativeMeasureNodeName, spec: SimpleMeasureItemSpec, parent: MeasureNode | Measure):
        parent_node_id=parent.id if not isinstance(parent, Measure) else None
        parent_measure_id=parent.id if isinstance(parent, Measure) else None
        return cls(
            id=id,
            name=MeasureNodeName(parent.name) / rel_name,
            parent_node_id=parent_node_id,
            parent_measure_id=parent_measure_id,
            studytable_id=None,
            prompt=spec.prompt,
            type=spec.type,
        )


MeasureItem = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
    ], Field(discriminator='type')
]

class MeasureItemGroup(StudyEntity):
    id: MeasureNodeId
    name: MeasureNodeName
    parent_node_id: t.Optional[MeasureNodeId]
    parent_measure_id: t.Optional[MeasureId]
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNode, ...]

    @classmethod
    def from_spec(cls, id: int, rel_name: RelativeMeasureNodeName, spec: MeasureItemGroupSpec, parent: MeasureNode | Measure):
        parent_node_id=parent.id if not isinstance(parent, Measure) else None
        parent_measure_id=parent.id if isinstance(parent, Measure) else None
        return cls(
            id=id,
            name=MeasureNodeName(parent.name) / rel_name,
            parent_node_id=parent_node_id,
            parent_measure_id=parent_measure_id,
            prompt=spec.prompt,
            type=spec.type,
            items=[],
        )

MeasureNode = t.Annotated[
    t.Union[
        MeasureItemGroup,
        MeasureItem,
    ], Field(discriminator='type')
]

MeasureItemGroup.update_forward_refs()

class Measure(StudyEntity):
    id: MeasureId
    name: MeasureName
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNode, ...]

### Instruments

class QuestionInstrumentItem(StudyEntity):
    id: InstrumentNodeId
    parent_node_id: InstrumentNodeId
    parent_instrument_id: InstrumentId
    measure_node_id: t.Optional[MeasureNodeId]
    index_column_id: t.Optional[IndexColumnId]
    source_column_name: SourceColumnName
    prompt: str
    type: t.Literal['question']

class ConstantInstrumentItem(StudyEntity):
    id: InstrumentNodeId
    parent_node_id: InstrumentNodeId
    parent_instrument_id: InstrumentId
    measure_node_id: t.Optional[MeasureNodeId]
    type: t.Literal['constant']
    value: str

class HiddenInstrumentItem(StudyEntity):
    id: InstrumentNodeId
    parent_node_id: InstrumentNodeId
    parent_instrument_id: InstrumentId
    measure_node_id: t.Optional[MeasureNodeId]
    source_column_name: SourceColumnName
    type: t.Literal['hidden']

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(StudyEntity):
    id: InstrumentNodeId
    parent_node_id: InstrumentNodeId
    parent_instrument_id: InstrumentId
    type: t.Literal['group']
    prompt: str
    title: str
    items: t.Tuple[InstrumentNode, ...]

InstrumentNode = t.Annotated[
    t.Union[
        InstrumentItem,
        InstrumentItemGroup,
    ], Field(discriminator='type')
]

InstrumentItemGroup.update_forward_refs()

class Instrument(StudyEntity):
    id: InstrumentId
    name: InstrumentName
    studytable_id: StudyTableId
    title: str
    description: t.Optional[str]
    items: t.Tuple[InstrumentNode, ...]

### Study Mutators

class AddEntityMutation(ImmutableBaseModel):
    entity: StudyEntity

class ConnectNodeToTable(ImmutableBaseModel):
    node_name: t.Union[MeasureNodeName, IndexColumnName]
    studytable_id: StudyTableId

StudyMutation = t.Union[
    AddEntityMutation,
    ConnectNodeToTable,
]

StudyMutationList = t.List[StudyMutation]