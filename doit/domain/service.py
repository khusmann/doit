import typing as t
from itertools import starmap

from itertools import count

from .value import *

def is_integer_text_column(values: t.Sequence[str | None]):
    return all([i.lstrip('-+').isdigit() for i in values if i is not None])

def sanitize_column(column: ColumnImport) -> SourceColumn:
    match column.type:
        case 'safe_bool':
            (column_type, values) = ('bool', column.values)
        case 'safe_text':
            (column_type, values) = ('text', column.values)
        case 'safe_ordinal':
            (column_type, values) = ('ordinal', column.values)
        case 'unsafe_text':
            # TODO: Sanitize
            column_type='text'
            values=[None for _ in column.values]
        case 'unsafe_numeric_text' if is_integer_text_column(column.values):
            # TODO: Sanitize
            column_type='integer'
            values=[None if i is None else int(i) for i in column.values]
        case 'unsafe_numeric_text':
            # TODO: Sanitize
            column_type='real'
            values=[None if i is None else float(i) for i in column.values],
            
    return new_source_column(
        column_id=column.column_id,
        meta=SourceColumnMeta(
            column_id=column.column_id,
            prompt=column.prompt,
            type=column_type,
            sanitizer_meta="TODO"
        ),
        column_type=column_type,
        values=values
    )

def sanitize_table(table: UnsafeSourceTable) -> SourceTable:
    safe_columns = { column_id: sanitize_column(column) for (column_id, column) in table.table.columns.items() }
    column_meta = { column_id: column.meta for (column_id, column) in safe_columns.items() }

    return SourceTable(
        instrument_id=table.instrument_id,
        columns=safe_columns,
        meta=SourceTableMeta(
            instrument_id=table.instrument_id,
            source_info=table.fetch_info,
            columns=column_meta,
        ),
    )

def stub_instrument_item(column_id: SourceColumnName, column: SourceColumn) -> InstrumentItemSpec:
    return QuestionInstrumentItemSpec(
        type='question',
        remote_id=column_id,
        measure_id=None,
        prompt=column.meta.prompt,
        map={ i: None for i in column.values if i is not None } if column.type=='ordinal' else None,
    )

def stub_instrument(table: SourceTable) -> InstrumentSpec:
    return InstrumentSpec(
        instrument_id=table.instrument_id,
        title=table.meta.source_info.title,
        description="description",
        instructions="instructions",
        items=list(starmap(stub_instrument_item, table.columns.items()))
    )

default_id_gen = count(0)

def study_from_spec(study_spec: StudySpec, id_gen: t.Iterator[int] = default_id_gen):
    mutations: t.List[StudyMutation] = []
    mutations += index_columns_from_spec(study_spec.config.indices, id_gen)
    mutations += measures_from_spec(study_spec.measures, id_gen)
    #mutations += instruments_from_spec(study_spec.instruments,  id_gen)

    context = CreationContext.from_mutations(mutations)

    entities = [
        m.create(context) for m in mutations
    ]

    print(entities)
    return mutations


def instruments_from_spec(
    instrument_specs: t.Mapping[InstrumentName, InstrumentSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[AddInstrumentMutator | AddInstrumentNodeMutator | AddStudyTableMutator]:

    mutations: t.List[AddInstrumentMutator | AddInstrumentNodeMutator | AddStudyTableMutator] = []

    studytable_lookup: t.Mapping[t.FrozenSet[IndexColumnName], AddStudyTableMutator] = {}

    for name, spec in instrument_specs.items():
        indices = frozenset(spec.index_column_names())

        studytable_mutation = studytable_lookup.get(indices)

        if studytable_mutation is None:
            studytable_mutation = (
                AddStudyTableMutator(
                    id=next(id_gen),
                    indices=indices,
                )
            )
            mutations += [studytable_mutation]

        instrument_mutation = AddInstrumentMutator(
            id=next(id_gen),
            name=name,
            spec=spec,
            studytable_id=studytable_mutation.id,
        )

        mutations += [instrument_mutation]

        mutations += instrument_nodes_from_spec(
            instrument_node_specs=spec.items,
            parent=instrument_mutation,
            studytable_id=studytable_mutation.id,
            id_gen=id_gen,
        )

    return mutations

def instrument_nodes_from_spec(
    instrument_node_specs: t.Sequence[InstrumentNodeSpec],
    parent: AddInstrumentMutator | AddInstrumentNodeMutator,
    studytable_id: StudyTableId,
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[AddInstrumentNodeMutator]:

    mutations: t.List[AddInstrumentNodeMutator] = []

    for spec in instrument_node_specs:
        match spec:
            case QuestionInstrumentItemSpec():
                mutations += [
                    AddQuestionInstrumentItemMutator(
                        id=next(id_gen),
                        parent=parent,
                        studytable_id=studytable_id,
                        spec=spec,
                    )
                ]
            case ConstantInstrumentItemSpec():
                mutations += [
                    AddConstantInstrumentItemMutator(
                        id=next(id_gen),
                        parent=parent,
                        studytable_id=studytable_id,
                        spec=spec,
                    )
                ]
            case HiddenInstrumentItemSpec():
                mutations += [
                    AddHiddenInstrumentItemMutator(
                        id=next(id_gen),
                        parent=parent,
                        studytable_id=studytable_id,
                        spec=spec,
                    )
                ]

            case InstrumentItemGroupSpec():
                group_mutation = (
                    AddInstrumentItemGroupMutator(
                        id=next(id_gen),
                        parent=parent,
                        studytable_id=studytable_id,
                        spec=spec,
                    )
                )
                mutations += [group_mutation]
                mutations += instrument_nodes_from_spec(spec.items, group_mutation, studytable_id, id_gen)


    return mutations

def index_columns_from_spec(
    index_column_specs: t.Mapping[IndexColumnName, IndexColumnSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[AddIndexColumnMutator | AddIndexCodeMapMutator]:

    codemaps = [
        AddIndexCodeMapMutator(
            id=next(id_gen),
            rel_name=name,
            spec=spec.values,
        ) for name, spec in index_column_specs.items()
    ]

    index_columns = [
        AddIndexColumnMutator(
            id=next(id_gen),
            name=name,
            spec=spec,
            codemap_id=codemap.id,
        ) for codemap, name, spec in zip(codemaps, index_column_specs.keys(), index_column_specs.values())
    ]

    return [*codemaps, *index_columns]

def measures_from_spec(
    measure_specs: t.Mapping[MeasureName, MeasureSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[AddMeasureMutator | AddCodeMapMutator | AddMeasureNodeMutator]:

    measures = [
        AddMeasureMutator(
            id=next(id_gen),
            name=name,
            spec=spec,
        ) for name, spec in measure_specs.items()
    ]

    codemaps = [
        AddCodeMapMutator(
            id=next(id_gen),
            rel_name=rel_name,
            root_measure_id=measure.id,
            spec=spec
        ) for spec, measure in zip(measure_specs.values(), measures)
            for rel_name, spec in spec.codes.items()
    ]

    nodes = sum([
        measure_nodes_from_spec(
            measure_node_specs=spec.items,
            parent_id=measure.id,
            root_measure_id=measure.id,
            id_gen=id_gen,
        ) for spec, measure in zip(measure_specs.values(), measures)
    ], [])

    return [*measures, *codemaps, *nodes]

def measure_nodes_from_spec(
    measure_node_specs: t.Mapping[RelativeMeasureNodeName, MeasureNodeSpec],
    parent_id: MeasureId | MeasureNodeId,
    root_measure_id: MeasureId,
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.List[AddMeasureNodeMutator]:
    
    measure_nodes = [
        AddMeasureNodeMutator(
            id=next(id_gen),
            rel_name=rel_name,
            parent_id=parent_id,
            root_measure_id=root_measure_id,
            spec=spec,
        ) for rel_name, spec in measure_node_specs.items()
    ]

    child_measure_nodes = sum([
        measure_nodes_from_spec(
            measure_node_specs=spec.items,
            parent_id=parent.id,
            root_measure_id=root_measure_id,
            id_gen=id_gen
        ) for spec, parent in zip(measure_node_specs.values(), measure_nodes) if spec.type == 'group'
    ], [])

    return [*measure_nodes, *child_measure_nodes]