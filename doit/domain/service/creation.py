from __future__ import annotations
import typing as t
from itertools import count, repeat
from functools import reduce
from ..value import *
from ..model import *

default_id_gen = count(0)

def mutations_from_study_spec(study_spec: StudySpec, table_info: t.Optional[t.Mapping[InstrumentName, SourceTableEntry]], id_gen: t.Iterator[int] = default_id_gen): # table_info: SourceTableInfo
    creators = [
        *index_column_creators_from_spec(study_spec.config.indices, id_gen),
        *measure_creators_from_spec(study_spec.measures, id_gen),
        *instrument_creators_from_spec(study_spec.instruments, id_gen),
    ]

    context = reduce(creation_context_reducer, creators, CreationContext(source_table_entry=table_info or {}))

    return [ creator.create(context) for creator in creators ]

def link_from_source_column(instrument_item: QuestionInstrumentItem, column: SourceColumn) -> t.Iterable[t.Any]:
    if instrument_item.map is None:
        return column.values
    else:
        return (instrument_item.map.get(str(v), v) for v in column.values)

def link_to_column_values(source: t.Iterable[t.Any], dest: ColumnInfoNodeContent) -> t.Iterable[t.Any]:
    if isinstance(dest, IndexColumn) or isinstance(dest, OrdinalMeasureItem):
        assert(dest.codemap is not None)
        cm = dest.codemap.tag_to_value_map()
        # TODO: What to do when a value is not in codemap
        return (cm.get(i) for i in source)
    else:
        match dest:
            case SimpleMeasureItem():
                return source
            case MeasureItemGroup():
                raise Exception("Instrument items cannot link to measure item groups")

def link_source_table(instrument: Instrument, source_table: SourceTable) -> AddSourceDataMutation:
    def link_values(instrument_item: InstrumentItem):
        assert(instrument_item.column_info_node is not None)
        if instrument_item.type == 'constant':
            source = repeat(instrument_item.value)
        else:
            # TODO: Handle situation when source_column_name is not in source_table.columns...
            assert(instrument_item.source_column_info is not None)
            source = link_from_source_column(instrument_item, source_table.columns[instrument_item.source_column_info.name])
        return link_to_column_values(source, instrument_item.column_info_node.content)

    return AddSourceDataMutation(
        studytable_id=instrument.studytable_id,
        columns={ i.column_info_node.name: link_values(i) for i in instrument.flat_items() if i.column_info_node is not None },
    )

#### Creators from Spec

def index_column_creators_from_spec(
    index_column_specs: t.Mapping[RelativeIndexColumnName, IndexColumnSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[IndexColumnCreator | IndexCodeMapCreator]:

    codemaps = [
        IndexCodeMapCreator(
            id=next(id_gen),
            rel_name=rel_name,
            spec=spec.values,
        ) for rel_name, spec in index_column_specs.items()
    ]

    index_columns = [
        IndexColumnCreator(
            id=next(id_gen),
            rel_name=rel_name,
            spec=spec,
            codemap_id=codemap.id,
        ) for (rel_name, spec), codemap in zip(index_column_specs.items(), codemaps)
    ]

    return [*codemaps, *index_columns]

def measure_creators_from_spec(
    measure_specs: t.Mapping[MeasureName, MeasureSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[MeasureCreator | CodeMapCreator | MeasureNodeCreator]:

    measures = [
        MeasureCreator(
            id=next(id_gen),
            name=name,
            spec=spec,
        ) for name, spec in measure_specs.items()
    ]

    codemaps = [
        CodeMapCreator(
            id=next(id_gen),
            rel_name=rel_name,
            root_measure_id=measure.id,
            spec=spec
        ) for spec, measure in zip(measure_specs.values(), measures)
            for rel_name, spec in spec.codes.items()
    ]

    measure_nodes = sum([
        measure_node_creators_from_spec(measure.id, id_gen)(
            measure_node_specs=spec.items,
            parent_node_id=None,
        ) for spec, measure in zip(measure_specs.values(), measures)
    ], [])

    return [*measures, *codemaps, *measure_nodes]

def measure_node_creators_from_spec(root_measure_id: MeasureId, id_gen: t.Iterator[int] = default_id_gen):
    def impl(
        measure_node_specs: t.Tuple[MeasureNodeSpec, ...],
        parent_node_id: t.Optional[ColumnInfoNodeId],
    ) -> t.List[MeasureNodeCreator]:
        
        measure_nodes = [
            MeasureNodeCreator(
                id=next(id_gen),
                parent_node_id=parent_node_id,
                root_measure_id=root_measure_id,
                spec=spec,
            ) for spec in measure_node_specs
        ]

        child_measure_nodes = sum([
            impl(spec.items, measure_node.id) 
                for spec, measure_node in zip(measure_node_specs, measure_nodes) if spec.type == 'group'
        ], [])

        return [*measure_nodes, *child_measure_nodes]
    return impl

def instrument_creators_from_spec(
    instrument_specs: t.Mapping[InstrumentName, InstrumentSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[InstrumentCreator | InstrumentNodeCreator | StudyTableCreator]:

    studytables: t.List[StudyTableCreator] = []
    studytable_lookup: t.Mapping[t.FrozenSet[RelativeIndexColumnName], StudyTableCreator] = {}
    for spec in instrument_specs.values():
        index_names = frozenset(spec.index_column_names())
        # Ensure we only create unique tables
        studytable = studytable_lookup.get(index_names) or (
            StudyTableCreator(
                id=next(id_gen),
                index_names=index_names,
            )
        )
        studytable_lookup |= { index_names: studytable }
        studytables += [studytable] # To point each instrument to a Studytable

    instruments = [
        InstrumentCreator(
            id=next(id_gen),
            name=name,
            spec=spec,
            studytable_id=studytable.id,
        ) for (name, spec), studytable in zip(instrument_specs.items(), studytables)
    ]

    instrument_nodes = sum([
        instrument_node_creators_from_spec(instrument.id, id_gen)(
            instrument_node_specs=spec.items,
            parent_node_id=None,
        ) for spec, instrument in zip(instrument_specs.values(), instruments)
    ], [])

    return [*set(studytables), *instruments, *instrument_nodes]

def instrument_node_creators_from_spec(root_instrument_id: InstrumentId, id_gen: t.Iterator[int] = default_id_gen):
    def impl(
        instrument_node_specs: t.Sequence[InstrumentNodeSpec],
        parent_node_id: t.Optional[InstrumentNodeId],
    ) -> t.List[InstrumentNodeCreator]:

        instrument_nodes = [
            InstrumentNodeCreator(
                id=next(id_gen),
                parent_node_id=parent_node_id,
                root_instrument_id=root_instrument_id,
                spec=spec,
            ) for spec in instrument_node_specs
        ]

        child_instrument_nodes = sum([
            impl(spec.items, instrument_node.id)
                for spec, instrument_node in zip(instrument_node_specs, instrument_nodes) if spec.type=='group'
        ], [])

        return [*instrument_nodes, *child_instrument_nodes]
    return impl

### CreationContext Reducer
#
# Here's where the magic happens
#
# The object copying / updating semantics of pydantic aren't type safe,
# not to mention really awkard :'(
#
# So we allow the reducer for CreationContext to directly modify the object
# but return "self" for faux-purity.

def creation_context_reducer(ctx: CreationContext, m: EntityCreator) -> CreationContext:
    # TODO: Include filename / root spec in creators,
    # so that errors can provide more debug info
    match m:
        case MeasureCreator():
            ctx.measure_name_by_id |= { m.id: m.name }

        case MeasureNodeCreator():
            base = (
                ColumnName(ctx.measure_name_by_id[m.root_measure_id]) if m.parent_node_id is None
                else ctx.column_info_node_name_by_id[m.parent_node_id]
            )
            ctx.column_info_node_name_by_id |= { m.id: base / m.spec.id }

        case IndexColumnCreator():
            column_name = ColumnName("indices") / m.rel_name
            ctx.index_column_name_by_rel_name |= { m.rel_name: column_name }
            ctx.column_info_node_name_by_id |= { m.id: column_name }

        case CodeMapCreator():
            base = ctx.measure_name_by_id[m.root_measure_id]
            ctx.codemap_name_by_id |= { m.id: CodeMapName(".".join([base, m.rel_name]))}
            ctx.codemap_id_by_measure_relname |= { (m.root_measure_id, m.rel_name): m.id }

        case IndexCodeMapCreator():
            ctx.codemap_name_by_id |= { m.id: CodeMapName(".".join(["indices", m.rel_name]))}

        case StudyTableCreator():
            ctx.studytable_name_by_id |= { m.id: StudyTableName("-".join(sorted(m.index_names)))}

        case InstrumentCreator():
            ctx.studytable_id_by_instrument_id |= { m.id: m.studytable_id }

        case InstrumentNodeCreator():
            spec = m.spec # Help the type checker resolve this...
            if spec.type != "group" and spec.id is not None:
                column_info_node_id = ctx.column_info_node_id_by_name[spec.id]
                studytable_id = ctx.studytable_id_by_instrument_id[m.root_instrument_id]
                existing_set = ctx.column_info_node_ids_by_studytable_id.get(studytable_id, frozenset())
                ctx.column_info_node_ids_by_studytable_id |= { studytable_id: existing_set | { column_info_node_id } }

    return ctx