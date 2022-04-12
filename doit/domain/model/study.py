from __future__ import annotations
import typing as t
from pydantic import Field

from ..value import *
from .sourcetable import SourceTableEntry, SourceColumnInfo

### CodeMap TODO: Elevate this somehow so DRY with CodeMapSpec

class CodeMap(ImmutableBaseModelOrm):
    id: CodeMapId
    name: CodeMapName
    values: t.Tuple[CodeMap.Value, ...]
    entity_type: t.Literal['codemap']

    class Value(t.TypedDict):
        value: CodeValue
        tag: CodeValueTag
        text: str

    def tag_to_value_map(self):
        return { v['tag']: v['value'] for v in self.values }

    def value_to_tag_map(self):
        return { v['value']: v['tag'] for v in self.values }

    def tag_to_text_map(self):
        return { v['tag']: v['text'] for v in self.values }

    def value_to_text_map(self):
        return { v['value']: v['text'] for v in self.values }

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
                values=self.spec.__root__,
                entity_type='codemap',
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
                values=self.spec.__root__,
                entity_type='codemap',
            )
        )

### Measures
class OrdinalMeasureItem(ImmutableBaseModelOrm):
    codemap_id: CodeMapId
    codemap: t.Optional[CodeMap]
    prompt: str
    type: OrdinalStudyColumnTypeStr

class SimpleMeasureItem(ImmutableBaseModelOrm):
    prompt: str
    type: SimpleStudyColumnTypeStr

class MeasureItemGroup(ImmutableBaseModelOrm):
    prompt: t.Optional[str]
    type: t.Literal['group']
    items: t.Optional[t.Tuple[ColumnInfoNode, ...]]

class IndexColumn(ImmutableBaseModelOrm):
    codemap_id: CodeMapId
    codemap: t.Optional[CodeMap]
    title: str
    description: t.Optional[str]
    type: t.Literal['index']

ColumnInfo = t.Annotated[
    t.Union[
        OrdinalMeasureItem,
        SimpleMeasureItem,
        IndexColumn,
    ], Field(discriminator='type')
]

ColumnInfoNodeContent = t.Annotated[
    t.Union[
        ColumnInfo,
        MeasureItemGroup,
    ], Field(discriminator='type')
]

class ColumnInfoNode(ImmutableBaseModelOrm):
    id: ColumnInfoNodeId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: t.Optional[MeasureId]
    entity_type: t.Literal['column_info_node']
    content: ColumnInfoNodeContent

class MeasureNodeCreator(ImmutableBaseModel):
    id: ColumnInfoNodeId
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId
    spec: MeasureNodeSpec

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=self.create_helper(ctx)
        )

    def create_helper(self, ctx: CreationContext) -> ColumnInfoNode:
        match self.spec:
            case OrdinalMeasureItemSpec():
                content = OrdinalMeasureItem(
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    codemap_id=ctx.codemap_id_by_measure_relname[(self.root_measure_id, self.spec.codes)],
                )
            case SimpleMeasureItemSpec():
                content = SimpleMeasureItem(
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )
            case MeasureItemGroupSpec():
                content = MeasureItemGroup(
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                )

        return ColumnInfoNode(
            id=self.id,
            name=ctx.column_info_node_name_by_id[self.id],
            parent_node_id=self.parent_node_id,
            root_measure_id=self.root_measure_id,
            entity_type='column_info_node',
            content=content,
        )

### IndexItem

class IndexColumnCreator(ImmutableBaseModel):
    id: ColumnInfoNodeId
    rel_name: RelativeIndexColumnName
    spec: IndexColumnSpec
    codemap_id: CodeMapId

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=ColumnInfoNode(
                id=self.id,
                name=ctx.column_info_node_name_by_id[self.id],
                entity_type='column_info_node',
                content=IndexColumn(
                    title=self.spec.title,
                    description=self.spec.description,
                    codemap_id=self.codemap_id,
                    type='index',
                )
            )
        )

### Measure

class Measure(ImmutableBaseModelOrm):
    id: MeasureId
    name: MeasureName
    title: str
    description: t.Optional[str]
    items: t.Tuple[ColumnInfoNode, ...]
    entity_type: t.Literal['measure']

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
                entity_type='measure',
            )
        )

### Instruments

class QuestionInstrumentItem(ImmutableBaseModelOrm):
    type: t.Literal['question']
    column_info_node_id: t.Optional[ColumnInfoNodeId]
    column_info_node: t.Optional[ColumnInfoNode]
    source_column_info: t.Optional[SourceColumnInfo]
    prompt: t.Optional[str]
    map: t.Optional[RecodeTransform]

class ConstantInstrumentItem(ImmutableBaseModelOrm):
    type: t.Literal['constant']
    column_info_id: t.Optional[ColumnInfoNodeId]
    column_info_node: t.Optional[ColumnInfoNode]
    value: str

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(ImmutableBaseModelOrm):
    type: t.Literal['group']
    prompt: t.Optional[str]
    title: t.Optional[str]
    items: t.Optional[t.Tuple[InstrumentNode, ...]]

InstrumentNodeContent = t.Annotated[
    t.Union[
        InstrumentItem,
        InstrumentItemGroup,
    ], Field(discriminator='type')
]

class InstrumentNode(ImmutableBaseModelOrm):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId
    entity_type: t.Literal['instrument_node']
    content: InstrumentNodeContent

class InstrumentNodeCreator(ImmutableBaseModel):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId
    spec: InstrumentNodeSpec

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=self.create_helper(ctx),
        )

    def create_helper(self, ctx: CreationContext) -> InstrumentNode:
        match self.spec:
            case QuestionInstrumentItemSpec():
                content = QuestionInstrumentItem(
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
                    source_column_name=self.spec.remote_id,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    map=self.spec.map,
                )
            case ConstantInstrumentItemSpec():
                content = ConstantInstrumentItem(
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
                    value=self.spec.value,
                    type=self.spec.type,
                )
            case InstrumentItemGroupSpec():
                content = InstrumentItemGroup(
                    prompt=self.spec.prompt,
                    title=self.spec.title,
                    type=self.spec.type,
                )
        return InstrumentNode(
            id=self.id,
            parent_node_id=self.parent_node_id,
            root_instrument_id=self.root_instrument_id,
            entity_type='instrument_node',
            content=content,
        )


### Instrument

class Instrument(ImmutableBaseModelOrm):
    id: InstrumentId
    name: InstrumentName
    studytable_id: StudyTableId
    title: str
    description: t.Optional[str]
    items: t.Optional[t.Tuple[InstrumentNode, ...]] # <- TODO: should these be empty instead of optional? Reducing None checks?
    entity_type: t.Literal['instrument']
    source_table_info: t.Optional[SourceTableInfo]

    def flat_items(self):
        def impl(nodes: t.Tuple[InstrumentNode, ...]) -> t.Generator[InstrumentItem, None, None]:
            for n in nodes:
                content = n.content
                if content.type == 'group':
                    if content.items is not None:
                        yield from impl(content.items)
                else:
                    yield content
        assert(self.items is not None)
        return impl(self.items)

class InstrumentCreator(ImmutableBaseModel):
    id: InstrumentId
    name: InstrumentName
    spec: InstrumentSpec
    studytable_id: StudyTableId

    def create(self, ctx: CreationContext) -> AddEntityMutation:
        source_table_entry = ctx.source_table_entries.get(self.name)
        return AddSimpleEntityMutation(
            entity=Instrument(
                id=self.id,
                name=self.name,
                studytable_id=self.studytable_id,
                title=self.spec.title,
                description=self.spec.description,
                entity_type='instrument',
                source_table_info=source_table_entry.content if source_table_entry else None
            )
        )

### StudyTable

class StudyTable(ImmutableBaseModelOrm):
    id: StudyTableId
    name: StudyTableName
    columns: t.Optional[t.Tuple[ColumnInfoNode, ...]]
    entity_type: t.Literal['studytable']

class StudyTableCreator(ImmutableBaseModel):
    id: StudyTableId
    index_names: t.FrozenSet[RelativeIndexColumnName]

    def create(self, ctx: CreationContext) -> AddEntityMutation:
        return AddStudyTableMutation(
            table=StudyTable(
                id=self.id,
                name=ctx.studytable_name_by_id[self.id],
                entity_type='studytable',
            ),
            column_info_node_ids=ctx.column_info_node_ids_by_studytable_id[self.id],
        )

### StudyEntities / Creators

StudyEntity = t.Annotated[
    t.Union[
        CodeMap,
        Measure,
        ColumnInfoNode,
        Instrument,
        InstrumentNode,
        StudyTable,
    ], Field(discriminator='entity_type')
]

NamedStudyEntity = t.Union[
    CodeMap,
    Measure,
    ColumnInfoNode,
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

class AddStudyTableMutation(ImmutableBaseModel):
    table: StudyTable
    column_info_node_ids: t.Set[ColumnInfoNodeId]

class AddSourceDataMutation(ImmutableBaseModel):
    studytable_id: StudyTableId
    columns: t.Mapping[ColumnName, t.Iterable[t.Any]]

AddEntityMutation = t.Union[
    AddSimpleEntityMutation,
    AddStudyTableMutation,
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
    source_table_entries: t.Mapping[InstrumentName, SourceTableEntry]
    codemap_id_by_measure_relname: t.Mapping[t.Tuple[MeasureId, RelativeCodeMapName], CodeMapId] = {}
    measure_name_by_id: t.Mapping[MeasureId, MeasureName] = {}
    codemap_name_by_id: t.Mapping[CodeMapId, CodeMapName] = {}
    column_info_node_name_by_id: t.Mapping[ColumnInfoNodeId, ColumnName] = {}
    studytable_name_by_id: t.Mapping[StudyTableId, StudyTableName] = {}
    studytable_id_by_instrument_id: t.Mapping[InstrumentId, StudyTableId] = {}
    index_column_name_by_rel_name: t.Mapping[RelativeIndexColumnName, ColumnName] = {}
    column_info_node_ids_by_studytable_id: t.Mapping[StudyTableId, t.FrozenSet[ColumnInfoNodeId]] = {}

    @property
    def column_info_node_id_by_name(self):
        return { name: id for (id, name) in self.column_info_node_name_by_id.items() }


# Update refs
CodeMap.update_forward_refs()
MeasureItemGroup.update_forward_refs()
InstrumentItemGroup.update_forward_refs()