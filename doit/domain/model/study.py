from __future__ import annotations
import typing as t
from pydantic import Field

from ..value import *
from .sourcetable import SourceTableInfo

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
class MeasureNodeBase(ImmutableBaseModelOrm):
    id: ColumnInfoNodeId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId
    entity_type: t.Literal['column_info_node']

class MeasureNodeBaseDict(t.TypedDict): # TODO: This is not needed if pydantic's BaseModel supports field unpacking...
    id: ColumnInfoNodeId
    name: ColumnName
    parent_node_id: t.Optional[ColumnInfoNodeId]
    root_measure_id: MeasureId
    entity_type: t.Literal['column_info_node']

class OrdinalMeasureItem(MeasureNodeBase):
    codemap_id: CodeMapId
    prompt: str
    type: OrdinalStudyColumnTypeStr
    codemap: t.Optional[CodeMap]

class SimpleMeasureItem(MeasureNodeBase):
    prompt: str
    type: SimpleStudyColumnTypeStr

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
            entity_type='column_info_node',
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
    entity_type: t.Literal['column_info_node']
    codemap: t.Optional[CodeMap]

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
                entity_type='column_info_node',
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

class InstrumentNodeBase(ImmutableBaseModelOrm):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId
    entity_type: t.Literal['instrument_node']

class InstrumentNodeBaseDict(t.TypedDict):
    id: InstrumentNodeId
    parent_node_id: t.Optional[InstrumentNodeId]
    root_instrument_id: InstrumentId
    entity_type: t.Literal['instrument_node']

class QuestionInstrumentItem(InstrumentNodeBase):
    column_info_id: t.Optional[ColumnInfoNodeId]
    column_info: t.Optional[ColumnInfo]
    source_column_name: SourceColumnName
    prompt: str
    type: t.Literal['question']
    map: t.Optional[RecodeTransform]

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
    map: t.Optional[RecodeTransform]

InstrumentItem = t.Annotated[
    t.Union[
        QuestionInstrumentItem,
        ConstantInstrumentItem,
        HiddenInstrumentItem,
    ], Field(discriminator='type')
]

class InstrumentItemGroup(InstrumentNodeBase):
    type: t.Literal['group']
    prompt: t.Optional[str]
    title: t.Optional[str]
    items: t.Optional[t.Tuple[InstrumentNode, ...]]

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

    def create(self, ctx: CreationContext) -> AddSimpleEntityMutation:
        return AddSimpleEntityMutation(
            entity=self.create_helper(ctx),
        )

    def create_helper(self, ctx: CreationContext) -> InstrumentNode:
        base = InstrumentNodeBaseDict(
            id=self.id,
            parent_node_id=self.parent_node_id,
            root_instrument_id=self.root_instrument_id,
            entity_type='instrument_node',
        )
        match self.spec:
            case QuestionInstrumentItemSpec():
                return QuestionInstrumentItem(
                    **base,
                    column_info_id=ctx.column_info_node_id_by_name.get(self.spec.id) if self.spec.id else None,
                    source_column_name=self.spec.remote_id,
                    prompt=self.spec.prompt,
                    type=self.spec.type,
                    map=self.spec.map,
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
                    map=self.spec.map,
                )
            case InstrumentItemGroupSpec():
                return InstrumentItemGroup(
                    **base,
                    prompt=self.spec.prompt,
                    title=self.spec.title,
                    type=self.spec.type,
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
    fetch_info: t.Optional[TableFetchInfo]

    def flat_items(self):
        def impl(nodes: t.Tuple[InstrumentNode, ...]) -> t.Generator[InstrumentItem, None, None]:
            for n in nodes:
                if n.type == 'group':
                    if n.items is not None:
                        yield from impl(n.items)
                else:
                    yield n
        assert(self.items is not None)
        return impl(self.items)

class InstrumentCreator(ImmutableBaseModel):
    id: InstrumentId
    name: InstrumentName
    spec: InstrumentSpec
    studytable_id: StudyTableId

    def create(self, ctx: CreationContext) -> AddEntityMutation:
        source_table_info = ctx.source_table_info.get(self.name)
        return AddSimpleEntityMutation(
            entity=Instrument(
                id=self.id,
                name=self.name,
                studytable_id=self.studytable_id,
                title=self.spec.title,
                description=self.spec.description,
                entity_type='instrument',
                fetch_info=source_table_info.fetch_info if source_table_info else None
            )
        )

### StudyTable

class StudyTable(ImmutableBaseModelOrm):
    id: StudyTableId
    name: StudyTableName
    columns: t.Optional[t.Tuple[ColumnInfo, ...]]
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
        MeasureNode,
        IndexColumn,
        Instrument,
        InstrumentNode,
        StudyTable,
    ], Field(discriminator='entity_type')
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
    source_table_info: t.Mapping[InstrumentName, SourceTableInfo]
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