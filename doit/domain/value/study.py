from __future__ import annotations
import typing as t

from .common import *
from pydantic import Field

from .studyspec import *

### CodeMap

class CodeMap(ImmutableBaseModelOrm):
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

    @classmethod
    def from_spec(cls, id: int, name: CodeMapName, spec: CodeMapSpec):
        return cls(
            id=id,
            name=name,
            values=spec.__root__,
        )

CodeMap.update_forward_refs()

### IndexItem

class IndexColumn(ImmutableBaseModelOrm):
    id: IndexColumnId
    name: IndexColumnName
    title: str
    description: t.Optional[str]
    codemap_id: CodeMapId

    @classmethod
    def from_spec(cls, id: int, name: IndexColumnName, spec: IndexColumnSpec, codemap_id: CodeMapId):
        return cls(
            id=id,
            name=name,
            title=spec.title,
            description=spec.description,
            codemap_id=codemap_id,
        )

### Measures
class MeasureNodeBase(ImmutableBaseModel):
    id: MeasureNodeId
    name: MeasureNodeName
    parent_node_id: t.Optional[MeasureNodeId]
    parent_measure_id: t.Optional[MeasureId]

    @classmethod
    def from_parent(cls, id: int, rel_name: RelativeMeasureNodeName, parent: MeasureNode | Measure):
        parent_node_id=parent.id if parent.type != 'root' else None
        parent_measure_id=parent.id if parent.type == 'root' else None
        return cls(
            id=id,
            name=MeasureNodeName(parent.name) / rel_name,
            parent_node_id=parent_node_id,
            parent_measure_id=parent_measure_id,           
        )
 

class OrdinalMeasureItem(MeasureNodeBase):

    studytable_id: t.Optional[StudyTableId]
    codemap_id: CodeMapId
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']

    @classmethod
    def from_spec(cls, base: MeasureNodeBase, spec: OrdinalMeasureItemSpec, codemap_id: CodeMapId):
        return cls(
            id=base.id,
            name=base.name,
            parent_node_id=base.parent_node_id,
            parent_measure_id=base.parent_measure_id,
            studytable_id=None,
            codemap_id=codemap_id,
            prompt=spec.prompt,
            type=spec.type,
        )

class SimpleMeasureItem(MeasureNodeBase):
    studytable_id: t.Optional[StudyTableId]
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

    @classmethod
    def from_spec(cls, base: MeasureNodeBase, spec: SimpleMeasureItemSpec):
        return cls(
            id=base.id,
            name=base.name,
            parent_node_id=base.parent_node_id,
            parent_measure_id=base.parent_measure_id,
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

class MeasureItemGroup(MeasureNodeBase):
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNode, ...]

    @classmethod
    def from_spec(cls, base: MeasureNodeBase, spec: MeasureItemGroupSpec):
        return cls(
            id=base.id,
            name=base.name,
            parent_node_id=base.parent_node_id,
            parent_measure_id=base.parent_measure_id,
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

class Measure(ImmutableBaseModelOrm):
    id: MeasureId
    name: MeasureName
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNode, ...]
    type: t.Literal['root'] = 'root'

    @classmethod
    def from_spec(cls, id: int, name: MeasureName, spec: MeasureSpec):
        return cls(
            id=id,
            name=name,
            title=spec.title,
            description=spec.description,
            items=[],
        )


### Instruments

class InstrumentNodeBase(ImmutableBaseModelOrm):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    parent_instrument_id: t.Optional[InstrumentId]

    @classmethod
    def from_parent(cls, id: int, parent: Instrument | InstrumentNode):
        parent_node_id = parent.id if parent.type != 'root' else None
        parent_instrument_id = parent.id if parent.type == 'root' else None
        return cls(
            id=id,
            parent_node_id=parent_node_id,
            parent_instrument_id=parent_instrument_id,
        )

class QuestionInstrumentItem(InstrumentNodeBase):
    measure_node_id: t.Optional[MeasureNodeId]
    index_column_id: t.Optional[IndexColumnId]
    source_column_name: SourceColumnName
    prompt: str
    type: t.Literal['question']

    @classmethod
    def from_spec(cls, base: InstrumentNodeBase, spec: QuestionInstrumentItemSpec):
        return cls(
            id=base.id,
            parent_node_id=base.parent_node_id,
            parent_instrument_id=base.parent_instrument_id,
            source_column_name=spec.remote_id,
            prompt=spec.prompt,
            type=spec.type,
        )

class ConstantInstrumentItem(InstrumentNodeBase):
    measure_node_id: t.Optional[MeasureNodeId]
    index_column_id: t.Optional[IndexColumnId]
    type: t.Literal['constant']
    value: str
    @classmethod
    def from_spec(cls, base: InstrumentNodeBase, spec: ConstantInstrumentItemSpec):
        return cls(
            id=base.id,
            parent_node_id=base.parent_node_id,
            parent_instrument_id=base.parent_instrument_id,
            value=spec.value,
            type=spec.type,
        )

class HiddenInstrumentItem(InstrumentNodeBase):
    measure_node_id: t.Optional[MeasureNodeId]
    index_column_id: t.Optional[IndexColumnId]
    source_column_name: SourceColumnName
    type: t.Literal['hidden']

    @classmethod
    def from_spec(cls, base: InstrumentNodeBase, spec: HiddenInstrumentItemSpec):
        return cls(
            id=base.id,
            parent_node_id=base.parent_node_id,
            parent_instrument_id=base.parent_instrument_id,
            source_column_name=spec.remote_id,
            type=type,
        )


InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(InstrumentNodeBase):
    type: t.Literal['group']
    prompt: str
    title: str
    items: t.Tuple[InstrumentNode, ...]

    @classmethod
    def from_spec(cls, base: InstrumentNodeBase, spec: InstrumentItemGroupSpec):
        return cls(
            id=base.id,
            parent_node_id=base.parent_node_id,
            parent_instrument_id=base.parent_instrument_id,
            type=spec.type,
            prompt=spec.prompt,
            title=spec.title,
            items=[],
        )


InstrumentNode = t.Annotated[
    t.Union[
        InstrumentItem,
        InstrumentItemGroup,
    ], Field(discriminator='type')
]

InstrumentItemGroup.update_forward_refs()

class Instrument(ImmutableBaseModelOrm):
    id: InstrumentId
    name: InstrumentName
    studytable_id: StudyTableId
    title: str
    description: t.Optional[str]
    items: t.Tuple[InstrumentNode, ...]
    type: t.Literal['root'] = 'root'

    @classmethod
    def from_spec(cls, id: int, name: InstrumentName, spec: InstrumentSpec, studytable_id: StudyTableId):
        return cls(
            id=id,
            name=name,
            studytable_id=studytable_id,
            title=spec.title,
            description=spec.description,
            items=(),
        )

### StudyTable

class StudyTable(ImmutableBaseModelOrm):
    id: StudyTableId
    name: StudyTableName
    index_names: t.Tuple[IndexColumnName, ...]
    measure_items: t.Tuple[MeasureItem, ...]

    @classmethod
    def from_indices(cls, id: int, indices: t.FrozenSet[IndexColumnName]):
        return cls(
            id=id,
            name="-".join(sorted(indices)),
            index_names=tuple(sorted(indices)),
            measure_items=(),
        )

### Study Mutators


class AddCodeMapMutator(ImmutableBaseModel):
    codemap: CodeMap

class AddMeasureMutator(ImmutableBaseModel):
    measure: Measure

class AddMeasureNodeMutator(ImmutableBaseModel):
    measure_node: MeasureNode

class AddIndexColumnMutator(ImmutableBaseModel):
    index_column: IndexColumn

class AddInstrumentMutator(ImmutableBaseModel):
    instrument: Instrument

class AddInstrumentNodeMutator(ImmutableBaseModel):
    instrument_node: InstrumentNode

class AddStudyTableMutator(ImmutableBaseModel):
    studytable: StudyTable

class ConnectColumnToTableMutator(ImmutableBaseModel):
    column_name: t.Union[MeasureNodeName, IndexColumnName]
    studytable_id: StudyTableId

class ConnectInstrumentNodeToColumnMutator(ImmutableBaseModel):
    node_id: InstrumentNodeId
    column_name: t.Union[MeasureNodeName, IndexColumnName]

StudyMutation = t.Union[
    AddCodeMapMutator,
    AddMeasureMutator,
    AddMeasureNodeMutator,
    AddIndexColumnMutator,
    AddInstrumentMutator,
    AddInstrumentNodeMutator,
    AddStudyTableMutator,
    ConnectColumnToTableMutator,
    ConnectInstrumentNodeToColumnMutator,
]