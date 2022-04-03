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

CodeMap.update_forward_refs()

class AddCodeMapMutator(ImmutableBaseModel):
    id: CodeMapId
    rel_name: RelativeCodeMapName
    root_measure_id: MeasureId
    spec: CodeMapSpec

    def create(self, context: CreationContext) -> CodeMap:
        return CodeMap(
            id=self.id,
            name=context.codemap_name_by_id[self.id],
            values=self.spec.__root__
        )

class AddIndexCodeMapMutator(ImmutableBaseModel):
    id: CodeMapId
    rel_name: IndexColumnName
    spec: CodeMapSpec

    def create(self, context: CreationContext) -> CodeMap:
        return CodeMap(
            id=self.id,
            name=context.codemap_name_by_id[self.id],
            values=self.spec.__root__
        )

### IndexItem

class IndexColumn(ImmutableBaseModelOrm):
    id: IndexColumnId
    name: IndexColumnName
    title: str
    description: t.Optional[str]

class AddIndexColumnMutator(ImmutableBaseModel):
    id: IndexColumnId
    name: IndexColumnName
    spec: IndexColumnSpec
    codemap_id: CodeMapId

    @property
    def codemap_name(self):
        return CodeMapName(".".join(['indices', self.name]))

    def create(self, context: CreationContext) -> IndexColumn:
        return IndexColumn(
            id=self.id,
            name=self.name,
            title=self.spec.title,
            description=self.spec.description,
            codemap_id=self.codemap_id,
        )

### Measures
class MeasureNodeBase(ImmutableBaseModel):
    id: MeasureNodeId
    name: MeasureNodeName
    parent_node_id: t.Optional[MeasureNodeId]
    parent_measure_id: t.Optional[MeasureId]

class OrdinalMeasureItem(MeasureNodeBase):
    studytable_id: t.Optional[StudyTableId]
    codemap_id: CodeMapId
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']

class SimpleMeasureItem(MeasureNodeBase):
    studytable_id: t.Optional[StudyTableId]
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

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

MeasureNode = t.Annotated[
    t.Union[
        MeasureItemGroup,
        MeasureItem,
    ], Field(discriminator='type')
]

MeasureItemGroup.update_forward_refs()

class AddMeasureNodeMutator(ImmutableBaseModel):
    id: MeasureNodeId
    rel_name: RelativeMeasureNodeName
    parent_id: t.Union[MeasureNodeId, MeasureId]
    root_measure_id: MeasureId
    spec: MeasureNodeSpec

    def create(self, context: CreationContext) -> MeasureNode:
        base = dict(
            id=self.id,
            name=context.measure_node_name_by_id[self.id],
            parent_node_id=self.parent_id if self.parent_id != self.root_measure_id else None,
            parent_measure_id=self.parent_id if self.parent_id == self.root_measure_id else None
        )
        match self.spec:
            case OrdinalMeasureItemSpec():
                return OrdinalMeasureItem(
                    **base,                    
                    studytable_id=context.studytable_id_by_measure_node_id.get(self.id),
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    codemap_id=context.codemap_id_by_measure_relname[(self.root_measure_id, self.spec.codes)],
                )
            case SimpleMeasureItemSpec():
                return SimpleMeasureItem(
                    **base,                    
                    studytable_id=context.studytable_id_by_measure_node_id.get(self.id),
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )
            case MeasureItemGroupSpec():
                return MeasureItemGroup(
                    **base,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    items=(),
                )

class Measure(ImmutableBaseModelOrm):
    id: MeasureId
    name: MeasureName
    title: str
    description: t.Optional[str]
    items: t.Tuple[MeasureNode, ...]

class AddMeasureMutator(ImmutableBaseModel):
    id: MeasureId
    name: MeasureName
    spec: MeasureSpec

    def create(self, _: CreationContext) -> Measure:
        return Measure(
            id=self.id,
            name=self.name,
            title=self.spec.title,
            description=self.spec.description,
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

class CreationContext(BaseModel):
    codemap_id_by_measure_relname: t.Mapping[t.Tuple[MeasureId, RelativeCodeMapName], CodeMapId] = {}
    measure_name_by_id: t.Mapping[MeasureId, MeasureName] = {}
    codemap_name_by_id: t.Mapping[CodeMapId, CodeMapName] = {}
    measure_node_name_by_id: t.Mapping[MeasureNodeId, MeasureNodeName] = {}

    @property
    def measure_id_by_name(self):
        return { name: id for (id, name) in self.measure_name_by_id.items() }

    studytable_id_by_columns: t.Mapping[t.FrozenSet[IndexColumnName], StudyTableId] = {}
    studytable_id_by_measure_node_id: t.Mapping[MeasureNodeId, StudyTableId] = {}

    def mutate(self, m: StudyMutation):
        match m:
            case AddMeasureMutator():
                self.measure_name_by_id |= { m.id: m.name }

            case AddMeasureNodeMutator():
                if m.root_measure_id == m.parent_id:
                    base = MeasureNodeName(self.measure_name_by_id[m.root_measure_id])
                else:
                    base = self.measure_node_name_by_id[MeasureNodeId(m.parent_id)]
                self.measure_node_name_by_id |= { m.id: base / m.rel_name }

            case AddCodeMapMutator():
                base = self.measure_name_by_id[m.root_measure_id]
                self.codemap_name_by_id |= { m.id: CodeMapName(".".join([base, m.rel_name]))}
                self.codemap_id_by_measure_relname |= { (m.root_measure_id, m.rel_name): m.id }

            case AddIndexCodeMapMutator():
                self.codemap_name_by_id |= { m.id: CodeMapName(".".join(["index", m.rel_name]))}

            case _:
                pass

    @classmethod
    def from_mutations(cls, mutations: t.List[StudyMutation]) -> CreationContext:
        ctx = cls()
        for m in mutations:
            ctx.mutate(m)
        return ctx


class AddInstrumentMutator(ImmutableBaseModel):
    id: InstrumentId
    name: InstrumentName
    spec: InstrumentSpec
    studytable_id: StudyTableId

    def create(self, _: CreationContext) -> Instrument:
        return Instrument.from_spec(
            self.id,
            self.name,
            self.spec,
            self.studytable_id,
        )

def instrument_node_base_helper(id: int, parent: AddInstrumentNodeMutator | AddInstrumentMutator):
    parent_node_id = parent.id if not isinstance(parent, AddInstrumentMutator) else None
    parent_instrument_id = parent.id if isinstance(parent, AddInstrumentMutator) else None
    return InstrumentNodeBase(
        id=id,
        parent_node_id=parent_node_id,
        parent_instrument_id=parent_instrument_id,
    )

class AddQuestionInstrumentItemMutator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent: t.Union[AddInstrumentMutator, AddInstrumentNodeMutator]
    spec: QuestionInstrumentItemSpec
    studytable_id: StudyTableId

    def create(self, context: CreationContext) -> QuestionInstrumentItem:
        base=instrument_node_base_helper(self.id, self.parent)
        return QuestionInstrumentItem.from_spec(base, self.spec)

class AddConstantInstrumentItemMutator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent: t.Union[AddInstrumentMutator, AddInstrumentNodeMutator]
    spec: ConstantInstrumentItemSpec
    studytable_id: StudyTableId

    def create(self, context: CreationContext) -> ConstantInstrumentItem:
        base=instrument_node_base_helper(self.id, self.parent)
        return ConstantInstrumentItem.from_spec(base, self.spec)

class AddHiddenInstrumentItemMutator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent: t.Union[AddInstrumentMutator, AddInstrumentNodeMutator]
    spec: HiddenInstrumentItemSpec
    studytable_id: StudyTableId

    def create(self, context: CreationContext) -> HiddenInstrumentItem:
        base=instrument_node_base_helper(self.id, self.parent)
        return HiddenInstrumentItem.from_spec(base, self.spec)

class AddInstrumentItemGroupMutator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent: t.Union[AddInstrumentMutator, AddInstrumentNodeMutator]
    spec: InstrumentItemGroupSpec
    studytable_id: StudyTableId

    def create(self, context: CreationContext) -> InstrumentItemGroup:
        base=instrument_node_base_helper(self.id, self.parent)
        return InstrumentItemGroup.from_spec(base, self.spec)

AddInstrumentNodeMutator = AddQuestionInstrumentItemMutator | AddConstantInstrumentItemMutator | AddHiddenInstrumentItemMutator | AddInstrumentItemGroupMutator

AddQuestionInstrumentItemMutator.update_forward_refs()
AddConstantInstrumentItemMutator.update_forward_refs()
AddHiddenInstrumentItemMutator.update_forward_refs()
AddInstrumentItemGroupMutator.update_forward_refs()



class AddStudyTableMutator(ImmutableBaseModel):
    id: StudyTableId
    indices: t.FrozenSet[IndexColumnName]

    def create(self, context: CreationContext) -> StudyTable:
        return StudyTable(
            id=self.id,
            name="-".join(sorted(self.indices)),
            indices=self.indices,
        )

StudyMutation = t.Union[
    AddCodeMapMutator,
    AddIndexCodeMapMutator,
    AddMeasureMutator,
    AddMeasureNodeMutator,
    AddIndexColumnMutator,
    AddInstrumentMutator,
    AddInstrumentNodeMutator,
    AddStudyTableMutator,
]