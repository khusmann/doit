from __future__ import annotations
import typing as t
from pydantic import Field

from .value.studyspec import *
from .value.common import *

### CodeMap TODO: Elevate this somehow so DRY with CodeMapSpec

class CodeMap(ImmutableBaseModelOrm):
    id: CodeMapId
    name: CodeMapName
    values: t.Tuple[CodeMap.Value, ...]

    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    def tag_to_value(self):
        return { v['tag']: v['value'] for v in self.values }

    def value_to_tag(self):
        return { v['value']: v['tag'] for v in self.values }

class CodeMapCreator(ImmutableBaseModel):
    id: CodeMapId
    rel_name: RelativeCodeMapName
    root_measure_id: MeasureId
    spec: CodeMapSpec

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=CodeMap(
                id=self.id,
                name=ctx.codemap_name_by_id[self.id],
                values=self.spec.__root__
            )
        )

class IndexCodeMapCreator(ImmutableBaseModel):
    id: CodeMapId
    rel_name: RelativeIndexColumnName
    spec: CodeMapSpec

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=CodeMap(
                id=self.id,
                name=ctx.codemap_name_by_id[self.id],
                values=self.spec.__root__
            )
        )

### Measures
class MeasureNodeBase(ImmutableBaseModelOrm):
    id: ColumnInfoNodeId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId

class MeasureNodeBaseDict(t.TypedDict): # TODO: This is not needed if pydantic's BaseModel supports field unpacking...
    id: ColumnInfoNodeId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId

class OrdinalMeasureItem(MeasureNodeBase):
    codemap_id: CodeMapId
    prompt: str
    type: t.Literal['ordinal', 'categorical', 'categorical_array']
    codemap: t.Optional[CodeMap]

class SimpleMeasureItem(MeasureNodeBase):
    prompt: str
    type: t.Literal['text', 'real', 'integer', 'bool']

class MeasureItemGroup(MeasureNodeBase):
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Optional[t.Tuple[MeasureNode, ...]]

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

class MeasureNodeCreator(ImmutableBaseModel):
    id: ColumnInfoNodeId
    rel_name: RelativeMeasureNodeName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId
    spec: MeasureNodeSpec

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=self.create_helper(ctx)
        )

    def create_helper(self, ctx: CreationContext) -> MeasureNode:
        base = MeasureNodeBaseDict(
            id=self.id,
            name=ctx.column_info_node_name_by_id[self.id],
            parent_node_id=self.parent_node_id,
            root_measure_id=self.root_measure_id,
        )
        match self.spec:
            case OrdinalMeasureItemSpec():
                return OrdinalMeasureItem(
                    **base,                   
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    codemap_id=ctx.codemap_id_by_measure_relname[(self.root_measure_id, self.spec.codes)],
                )
            case SimpleMeasureItemSpec():
                return SimpleMeasureItem(
                    **base,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )
            case MeasureItemGroupSpec():
                return MeasureItemGroup(
                    **base,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )

### IndexItem

class IndexColumn(ImmutableBaseModelOrm):
    id: ColumnInfoNodeId
    name: ColumnName
    title: str
    description: t.Optional[str]
    codemap_id: CodeMapId
    type: t.Literal['index']

class IndexColumnCreator(ImmutableBaseModel):
    id: ColumnInfoNodeId
    rel_name: RelativeIndexColumnName
    spec: IndexColumnSpec
    codemap_id: CodeMapId

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=IndexColumn(
                id=self.id,
                name=ctx.column_info_node_name_by_id[self.id],
                title=self.spec.title,
                description=self.spec.description,
                codemap_id=self.codemap_id,
                type='index',
            )
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

class MeasureCreator(ImmutableBaseModel):
    id: MeasureId
    name: MeasureName
    spec: MeasureSpec

    def create(self, _: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=Measure(
                id=self.id,
                name=self.name,
                title=self.spec.title,
                description=self.spec.description,
                items=(),
            )
        )

### Instruments

class InstrumentNodeBase(ImmutableBaseModelOrm):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId

class InstrumentNodeBaseDict(t.TypedDict):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId

class QuestionInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoNodeId]
    column_info: t.Optional[ColumnInfo]
    source_column_name: SourceColumnName
    prompt: str
    type: t.Literal['question']

class ConstantInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoNodeId]
    column_info: t.Optional[ColumnInfo]
    type: t.Literal['constant']
    value: str

class HiddenInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoNodeId]
    column_info: t.Optional[ColumnInfo]
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

class InstrumentNodeCreator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId
    spec: InstrumentNodeSpec

    def create(self, ctx: CreationContext) -> AddInstrumentNodeMutation:
        return AddInstrumentNodeMutation(
            instrument_node=self.create_helper(ctx),
            association=self.association_helper(ctx),
        )

    def association_helper(self, ctx: CreationContext) -> t.Optional[AddInstrumentNodeMutation.StudyTableAssociation]:
        spec = self.spec # Create temp local var to help type checker...
        return AddInstrumentNodeMutation.StudyTableAssociation(
            studytable_id=ctx.studytable_id_by_instrument_id[self.root_instrument_id],
            column_info_node_id=ctx.column_info_node_id_by_name[spec.id]
        ) if not (spec.type == 'group') and (spec.id is not None) else None

    def create_helper(self, ctx: CreationContext) -> InstrumentNode:
        base = InstrumentNodeBaseDict(
            id=self.id,
            parent_node_id=self.parent_node_id,
            root_instrument_id=self.root_instrument_id,
        )
        match self.spec:
            case QuestionInstrumentItemSpec():
                return QuestionInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
                    source_column_name=self.spec.remote_id,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )
            case ConstantInstrumentItemSpec():
                return ConstantInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
                    value=self.spec.value,
                    type=self.spec.type,
                )
            case HiddenInstrumentItemSpec():
                return HiddenInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
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

class InstrumentCreator(ImmutableBaseModel):
    id: InstrumentId
    name: InstrumentName
    spec: InstrumentSpec
    studytable_id: StudyTableId

    def create(self, _: CreationContext) -> StudyMutation:
        return AddSimpleEntityMutation(
            entity=Instrument(
                id=self.id,
                name=self.name,
                studytable_id=self.studytable_id,
                title=self.spec.title,
                description=self.spec.description,
                items=(),
            )
        )

### StudyTable

class StudyTable(ImmutableBaseModelOrm):
    id: StudyTableId
    name: StudyTableName
    columns: t.Tuple[ColumnInfo, ...] = ()

class StudyTableCreator(ImmutableBaseModel):
    id: StudyTableId
    index_names: t.FrozenSet[RelativeIndexColumnName]

    def create(self, ctx: CreationContext) -> StudyMutation:
        return AddSimpleEntityMutation(
            entity = StudyTable(
                id=self.id,
                name=ctx.studytable_name_by_id[self.id],
            )
        )

### StudyEntities / Creators

StudyEntity = t.Union[
    CodeMap,
    Measure,
    MeasureNode,
    IndexColumn,
    Instrument,
    InstrumentNode,
    StudyTable,
]

NamedStudyEntity = t.Union[
    CodeMap,
    Measure,
    MeasureNode,
    IndexColumn,
    MeasureItem,
    Instrument,
    # Instrument Nodes don't have unique names
    StudyTable,
]

EntityCreator = t.Union[
    CodeMapCreator,
    IndexCodeMapCreator,
    MeasureCreator,
    MeasureNodeCreator,
    IndexColumnCreator,
    InstrumentCreator,
    InstrumentNodeCreator,
    StudyTableCreator,
]

### Study Mutators

class AddSimpleEntityMutation(ImmutableBaseModel):
    entity: StudyEntity

class AddInstrumentNodeMutation(ImmutableBaseModel):
    instrument_node: InstrumentNode
    association: t.Optional[AddInstrumentNodeMutation.StudyTableAssociation]
    
    class StudyTableAssociation(t.TypedDict):
        studytable_id: StudyTableId
        column_info_node_id: ColumnInfoNodeId

class AddSourceDataMutation(ImmutableBaseModel):
    table_id: StudyTableId
    indices: t.Mapping[ColumnName, t.Tuple[int, ...]]
    columns: t.Mapping[ColumnName, t.Tuple[t.Any, ...]]

StudyMutation = t.Union[
    AddSimpleEntityMutation,
    AddInstrumentNodeMutation,
    AddSourceDataMutation,
]

### CreationContext

# The object copying / updating semantics of pydantic aren't type safe,
# not to mention really awkard :'(
#
# So we allow the reducer for CreationContext to directly modify the object
# but return "self" for faux-purity.
# 
# Therefore here we inherit from BaseModel instead of ImmuntableBaseModel.

class CreationContext(BaseModel):
    codemap_id_by_measure_relname: t.Mapping[t.Tuple[MeasureId, RelativeCodeMapName], CodeMapId] = {}
    measure_name_by_id: t.Mapping[MeasureId, MeasureName] = {}
    codemap_name_by_id: t.Mapping[CodeMapId, CodeMapName] = {}
    column_info_node_name_by_id: t.Mapping[ColumnInfoNodeId, ColumnName] = {}
    studytable_name_by_id: t.Mapping[StudyTableId, StudyTableName] = {}
    studytable_id_by_instrument_id: t.Mapping[InstrumentId, StudyTableId] = {}
    index_column_name_by_rel_name: t.Mapping[RelativeIndexColumnName, ColumnName] = {}
    studytable_id_by_measure_node_id: t.Mapping[ColumnInfoNodeId, StudyTableId] = {}

    @property
    def column_info_node_id_by_name(self):
        return { name: id for (id, name) in self.column_info_node_name_by_id.items() }


# Update refs
CodeMap.update_forward_refs()
AddInstrumentNodeMutation.update_forward_refs()
MeasureItemGroup.update_forward_refs()
InstrumentItemGroup.update_forward_refs()