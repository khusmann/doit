import typing as t
from itertools import starmap, groupby

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

def flatten_instrument_items(items: t.Sequence[InstrumentNodeSpec]) -> t.List[InstrumentItemSpec]:
    return sum([ flatten_instrument_items(i.items) if i.type == 'group' else [i] for i in items], [])

def flatten_measures(measures: t.Mapping[MeasureName, MeasureSpec]) -> t.Dict[MeasureNodeName, MeasureItemSpec]:
    def trav(curr_path: MeasureNodeName, cursor: t.Mapping[RelativeMeasureNodeName, MeasureNodeSpec]) -> t.Dict[MeasureNodeName, MeasureItemSpec]:
        return merge_mappings([
            trav(curr_path / id, item.items) if item.type == 'group' else {(curr_path / id): item}
            for (id, item) in cursor.items()
        ])
    return merge_mappings([
        trav(MeasureNodeName(id), measure.items) for (id, measure) in measures.items()
    ])

def instrument_to_table_spec(instrument_spec: InstrumentSpec) -> TableSpec:
    all_columns = frozenset((i.id for i in flatten_instrument_items(instrument_spec.items) if i.id is not None))
    indices = frozenset((tag.removeprefix('indices.') for tag in all_columns if tag.startswith('indices.')))
    columns = frozenset((tag for tag in all_columns if not tag.startswith('index.')))
    return TableSpec(
        indices=indices,
        columns=columns,
    )

def instruments_to_table_specs(instruments: t.Sequence[InstrumentSpec]) -> t.Set[TableSpec]:
    return { 
        TableSpec(
            indices=k,
            columns=frozenset(sum(map(lambda i: list(i.columns), v), []))
        )
        for k, v in groupby(
            map(instrument_to_table_spec, instruments),
            lambda i: i.indices,
        ) 
    }

def link_column(instrument_item: InstrumentItemSpec, source_columns: t.Mapping[SourceColumnName, SourceColumn],  measure_items: t.Mapping[MeasureNodeName, MeasureItemSpec]) -> LinkedColumn:
    match instrument_item:
        case QuestionInstrumentItemSpec():
            assert(instrument_item.id is not None)
            src = source_columns[instrument_item.remote_id]
            m = measure_items[instrument_item.id]
            match (src, m):
                #case (SourceColumnBase(type='ordinal'), IndexSpec())
                case (SourceColumnBase(type='ordinal'), OrdinalMeasureItemSpec()):
                    pass
                case (SourceColumnBase(), SimpleMeasureItemSpec()):
                    return LinkedColumn(
                        column_id=instrument_item.id,
                        type=src.type,
                        values=(5, 6)
                    )
                case _:
                    pass
        case ConstantInstrumentItemSpec():
            pass
        case _:
            pass
    raise Exception("Not implemented")

def link_table(source: SourceTable, instrument_spec: InstrumentSpec, measures: t.Mapping[MeasureName, MeasureSpec]) -> LinkedTable:
    assert(source.instrument_id == instrument_spec.instrument_id)
    flat_measure_items = flatten_measures(measures)
    return LinkedTable(
        instrument_id=source.instrument_id,
        table_id=instrument_to_table_spec(instrument_spec).tag,
        columns=tuple((link_column(instrument_item, source.columns, flat_measure_items) for instrument_item in flatten_instrument_items(instrument_spec.items)))
    )

default_id_gen = count(0)

def measures_from_spec(
    measure_specs: t.Mapping[MeasureName, MeasureSpec],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.Tuple[StudyMutationList, t.Dict[MeasureName, MeasureId]]:

    lookup: t.Dict[MeasureName, MeasureId] = {}
    mutations: StudyMutationList = []

    for name, spec in measure_specs.items():
        measure = Measure(
            id=next(id_gen),
            name=name,
            title=spec.title,
            description=spec.description,
            items=[],
        )

        codemap_mutations, codemap_lookup = codemaps_from_spec(
            codemap_specs=spec.codes,
            parent_name=name,
            id_gen=id_gen,
        )

        node_mutations, _ = measure_nodes_from_spec(
            measure_node_specs=spec.items,
            parent=measure,
            codemap_lookup=codemap_lookup,
            id_gen=id_gen,
        )

        mutations += [AddEntityMutation(entity=measure)]
        mutations += codemap_mutations
        mutations += node_mutations

        lookup |= { name: measure.id }

    return mutations, lookup

def measure_nodes_from_spec(
    measure_node_specs: t.Mapping[RelativeMeasureNodeName, MeasureNodeSpec],
    parent: Measure | MeasureNode,
    codemap_lookup: t.Mapping[RelativeCodeMapName, CodeMapId],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.Tuple[StudyMutationList, t.Dict[MeasureNodeName, MeasureNodeId]]:

    lookup: t.Dict[MeasureNodeName, MeasureNodeId] = {}
    mutations: StudyMutationList = []

    for rel_name, spec in measure_node_specs.items():
        match spec:
            case OrdinalMeasureItemSpec():
                node = OrdinalMeasureItem.from_spec(next(id_gen), rel_name, spec, parent, codemap_lookup)
            case SimpleMeasureItemSpec():
                node = SimpleMeasureItem.from_spec(next(id_gen), rel_name, spec, parent)
            case MeasureItemGroupSpec():
                node = MeasureItemGroup.from_spec(next(id_gen), rel_name, spec, parent)

                child_mutations, child_lookup = measure_nodes_from_spec(
                    measure_node_specs=spec.items,
                    parent=node,
                    codemap_lookup=codemap_lookup,
                    id_gen=id_gen,
                )

                mutations += child_mutations
                lookup |= child_lookup

        mutations += [AddEntityMutation(entity=node)]
        lookup |= { node.name: node.id }
    return mutations, lookup



def codemaps_from_spec(
    codemap_specs: t.Mapping[RelativeCodeMapName, CodeMapSpec],
    parent_name: MeasureName | t.Literal['index'],
    id_gen: t.Iterator[int] = default_id_gen,
) -> t.Tuple[StudyMutationList, t.Dict[RelativeCodeMapName, CodeMapId]]:

    lookup: t.Dict[RelativeCodeMapName, CodeMapId] = {}
    mutations: StudyMutationList = []
    for rel_name, spec in codemap_specs.items():
        codemap = CodeMap(
            id=next(id_gen),
            name=CodeMapName(".".join([parent_name, rel_name])),
            values=spec.__root__,
        )
        mutations += [AddEntityMutation(entity=codemap)]
        lookup |= { rel_name: codemap.id }

    return mutations, lookup