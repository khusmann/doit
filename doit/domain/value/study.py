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

    def create(self, ctx: CreationContext) -> CodeMap:
        return CodeMap(
            id=self.id,
            name=ctx.codemap_name_by_id[self.id],
            values=self.spec.__root__
        )

class AddIndexCodeMapMutator(ImmutableBaseModel):
    id: CodeMapId
    rel_name: RelativeIndexColumnName
    spec: CodeMapSpec

    def create(self, ctx: CreationContext) -> CodeMap:
        return CodeMap(
            id=self.id,
            name=ctx.codemap_name_by_id[self.id],
            values=self.spec.__root__
        )

### Measures
class MeasureNodeBase(ImmutableBaseModel):
    id: ColumnInfoId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoId]
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

class MeasureItemGroup(MeasureNodeBase):
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Tuple[MeasureNode, ...]

MeasureItem = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
    ], Field(discriminator='type')
]

MeasureNode = t.Annotated[
    t.Union[
        MeasureItemGroup,
        MeasureItem,
    ], Field(discriminator='type')
]

MeasureItemGroup.update_forward_refs()

class AddMeasureNodeMutator(ImmutableBaseModel):
    id: ColumnInfoId
    rel_name: RelativeMeasureNodeName
    parent_id: t.Union[ColumnInfoId, MeasureId]
    root_measure_id: MeasureId
    spec: MeasureNodeSpec

    def create(self, ctx: CreationContext) -> MeasureNode:
        base = dict(
            id=self.id,
            name=ctx.column_info_name_by_id[self.id],
            parent_node_id=self.parent_id if self.parent_id != self.root_measure_id else None,
            parent_measure_id=self.parent_id if self.parent_id == self.root_measure_id else None
        )
        match self.spec:
            case OrdinalMeasureItemSpec():
                return OrdinalMeasureItem(
                    **base,                    
                    studytable_id=ctx.studytable_id_by_measure_node_id.get(self.id),
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    codemap_id=ctx.codemap_id_by_measure_relname[(self.root_measure_id, self.spec.codes)],
                )
            case SimpleMeasureItemSpec():
                return SimpleMeasureItem(
                    **base,                    
                    studytable_id=ctx.studytable_id_by_measure_node_id.get(self.id),
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

### IndexItem

class IndexColumn(ImmutableBaseModelOrm):
    id: ColumnInfoId
    name: ColumnName
    title: str
    description: t.Optional[str]
    codemap_id: CodeMapId
    type: t.Literal['index']

class AddIndexColumnMutator(ImmutableBaseModel):
    id: ColumnInfoId
    rel_name: RelativeIndexColumnName
    spec: IndexColumnSpec
    codemap_id: CodeMapId

    def create(self, ctx: CreationContext) -> IndexColumn:
        return IndexColumn(
            id=self.id,
            name=ctx.column_info_name_by_id[self.id],
            title=self.spec.title,
            description=self.spec.description,
            codemap_id=self.codemap_id,
            type='index',
        )

ColumnInfo = t.Annotated[
    t.Union[
        MeasureItem,
        IndexColumn,
    ], Field(discriminator='type')
]

### Measure

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

class QuestionInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoId]
    source_column_name: SourceColumnName
    prompt: str
    type: t.Literal['question']

class ConstantInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoId]
    type: t.Literal['constant']
    value: str

class HiddenInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoId]
    source_column_name: SourceColumnName
    type: t.Literal['hidden']

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

InstrumentNode = t.Annotated[
    t.Union[
        InstrumentItem,
        InstrumentItemGroup,
    ], Field(discriminator='type')
]

InstrumentItemGroup.update_forward_refs()

class AddInstrumentNodeMutator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent_id: t.Union[InstrumentNodeId, InstrumentNode]
    root_instrument_id: InstrumentId
    spec: InstrumentNodeSpec

    def create(self, ctx: CreationContext) -> InstrumentNode:
        base = dict(
            id=self.id,
            parent_node_id=self.parent_id if self.parent_id != self.root_instrument_id else None,
            parent_instrument_id=self.parent_id if self.parent_id == self.root_instrument_id else None
        )

        match self.spec:
            case QuestionInstrumentItemSpec():
                return QuestionInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_id_by_name.get(self.spec.id) if self.spec.id else None,
                    source_column_name=self.spec.remote_id,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )
            case ConstantInstrumentItemSpec():
                return ConstantInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_id_by_name.get(self.spec.id) if self.spec.id else None,
                    value=self.spec.value,
                    type=self.spec.type,
                )
            case HiddenInstrumentItemSpec():
                return HiddenInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_id_by_name.get(self.spec.id) if self.spec.id else None,
                    source_column_name=self.spec.remote_id,
                    type=self.spec.type,
                )
            case InstrumentItemGroupSpec():
                return InstrumentItemGroup(
                    **base,
                    prompt=self.spec.prompt,
                    title=self.spec.title,
                    type=self.spec.type,
                    items=(),
                )

### Instrument

class Instrument(ImmutableBaseModelOrm):
    id: InstrumentId
    name: InstrumentName
    studytable_id: StudyTableId
    title: str
    description: t.Optional[str]
    items: t.Tuple[InstrumentNode, ...]

class AddInstrumentMutator(ImmutableBaseModel):
    id: InstrumentId
    name: InstrumentName
    spec: InstrumentSpec
    studytable_id: StudyTableId

    def create(self, _: CreationContext) -> Instrument:
        return Instrument(
            id=self.id,
            name=self.name,
            studytable_id=self.studytable_id,
            title=self.spec.title,
            description=self.spec.description,
            items=(),
        )

### StudyTable

class StudyTable(ImmutableBaseModelOrm):
    id: StudyTableId
    name: StudyTableName
    index_names: t.Tuple[RelativeIndexColumnName, ...]
    measure_items: t.Tuple[MeasureNode, ...]

class AddStudyTableMutator(ImmutableBaseModel):
    id: StudyTableId
    index_names: t.FrozenSet[RelativeIndexColumnName]

    def create(self, ctx: CreationContext) -> StudyTable:
        return StudyTable(
            id=self.id,
            name=ctx.studytable_name_by_id[self.id],
            index_names=tuple(sorted(self.index_names)),
            measure_items=(),
        )

### Study Mutators

class CreationContext(BaseModel):
    codemap_id_by_measure_relname: t.Mapping[t.Tuple[MeasureId, RelativeCodeMapName], CodeMapId] = {}
    measure_name_by_id: t.Mapping[MeasureId, MeasureName] = {}
    codemap_name_by_id: t.Mapping[CodeMapId, CodeMapName] = {}
    column_info_name_by_id: t.Mapping[ColumnInfoId, ColumnName] = {}
    studytable_name_by_id: t.Mapping[StudyTableId, StudyTableName] = {}
    studytable_id_by_measure_node_id: t.Mapping[ColumnInfoId, StudyTableId] = {}
    studytable_id_by_instrument_id: t.Mapping[InstrumentId, StudyTableId] = {}

    @property
    def column_info_id_by_name(self):
        return { name: id for (id, name) in self.column_info_name_by_id.items() }

    def mutate(self, m: StudyMutation):
        match m:
            case AddMeasureMutator():
                self.measure_name_by_id |= { m.id: m.name }

            case AddMeasureNodeMutator():
                if m.root_measure_id == m.parent_id:
                    base = ColumnName(self.measure_name_by_id[m.root_measure_id])
                else:
                    base = self.column_info_name_by_id[ColumnInfoId(m.parent_id)]
                self.column_info_name_by_id |= { m.id: base / m.rel_name }

            case AddIndexColumnMutator():
                self.column_info_name_by_id |= { m.id: ColumnName("indices") / m.rel_name }

            case AddCodeMapMutator():
                base = self.measure_name_by_id[m.root_measure_id]
                self.codemap_name_by_id |= { m.id: CodeMapName(".".join([base, m.rel_name]))}
                self.codemap_id_by_measure_relname |= { (m.root_measure_id, m.rel_name): m.id }

            case AddIndexCodeMapMutator():
                self.codemap_name_by_id |= { m.id: CodeMapName(".".join(["indices", m.rel_name]))}

            case AddStudyTableMutator():
                self.studytable_name_by_id |= { m.id: StudyTableName("-".join(sorted(m.index_names)))}

            case AddInstrumentMutator():
                self.studytable_id_by_instrument_id |= { m.id: m.studytable_id }

            case AddInstrumentNodeMutator():
                spec = m.spec
                if spec.type != "group" and spec.id:
                    if spec.id.startswith("indices."):
                        pass
                    else:
                        measure_node_id = self.column_info_id_by_name[spec.id]
                        studytable_id = self.studytable_id_by_instrument_id[m.root_instrument_id]
                        existing_val = self.studytable_id_by_measure_node_id.get(measure_node_id)
                        if existing_val is not None and existing_val != studytable_id:
                            raise Exception("Error measure items must only belong to one table. (Measure: {})".format(spec.id))
                        self.studytable_id_by_measure_node_id |= { measure_node_id: studytable_id }

            case _:
                pass

    @classmethod
    def from_mutations(cls, mutations: t.List[StudyMutation]) -> CreationContext:
        result = cls()
        for m in mutations:
            result.mutate(m)
        return result

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

StudyEntity = t.Union[
    CodeMap,
    Measure,
    MeasureNode,
    IndexColumn,
    Instrument,
    InstrumentNode,
    StudyTable,
]