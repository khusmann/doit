from __future__ import annotations
import typing as t
from faker import Faker
from faker.providers import BaseProvider
from doit.common.table import OrdinalValue

from dataclasses import dataclass

from doit.study.spec import (
    ConstantInstrumentItemSpec,
    IndexColumnSpec,
    InstrumentItemGroupSpec,
    InstrumentNodeSpec,
    InstrumentSpec,
    MeasureItemGroupSpec,
    MeasureNodeSpec,
    MeasureSpec,
    OrdinalMeasureItemSpec,
    QuestionInstrumentItemSpec,
    RelativeCodeMapName,
    RelativeMeasureNodeName,
    SimpleMeasureItemSpec,
    StudyConfigSpec,
    RelativeIndexColumnName,
    CodeMapSpec,
    StudySpec,
)

fake = Faker()

def make_measure_node_pool(measure_map: t.Mapping[str, MeasureSpec]) -> t.Tuple[t.Mapping[str, MeasureNodeSpec], ...]:
    def inner(items: t.Tuple[MeasureNodeSpec], parent_name: str) -> t.Tuple[t.Tuple[str, MeasureNodeSpec], ...]:
        children = tuple(
            (".".join((parent_name, i.id)), i)
                for i in items
                    if not isinstance(i, MeasureItemGroupSpec)
        )
        grandchildren = tuple(
            j
                for i in items
                    if isinstance(i, MeasureItemGroupSpec)
                        for j in inner(i.items, ".".join((parent_name, i.id)))
        )
        return children + grandchildren
    return tuple(
        dict(inner(measure.items, measure_name))
            for measure_name, measure in measure_map.items()
    )

def make_index_column_pool(index_column_map: t.Mapping[RelativeIndexColumnName, IndexColumnSpec]) -> t.Mapping[str, IndexColumnSpec]:
    return {
        "indices.{}".format(k): v for k, v in index_column_map.items()
    }

@dataclass
class InstrumentCreationContext:
    parent: StudySpecProvider
    measure_node_pools: t.Collection[t.Mapping[str, MeasureNodeSpec]]
    index_pool: t.Mapping[str, IndexColumnSpec]

    def instrument_map(self, n: int = 4) -> t.Mapping[str, InstrumentSpec]:
        return {
            self.instrument_name(): self.instrument()
                for _ in range(n)
        }

    def instrument_name(self):
        return "instrument_{}".format(fake.unique.word())

    def instrument(self) -> InstrumentSpec:
        measure_pool: t.Mapping[str, MeasureNodeSpec] = self.parent.random_element(self.measure_node_pools) # type: ignore

        question_pool = tuple(
            self.question_instrument_node(i) for i in measure_pool.items()
        ) 

        constant_pool = tuple(
            self.constant_instrument_node(i) for i in measure_pool.items()
        )

        pool: t.Tuple[InstrumentNodeSpec] = tuple(
            self.parent.random_element(p) # type: ignore
                for p in zip(question_pool, constant_pool)
        ) + tuple(self.question_instrument_node(None) for _ in range(int(len(measure_pool)/4)))
        
        return InstrumentSpec(
            title=fake.text(20),
            description=fake.sentence(20),
            instructions=fake.sentence(20),
            items=self.instrument_node_tree(pool)
        )

    def question_instrument_node(self, connect_node: t.Optional[t.Tuple[str, MeasureNodeSpec | IndexColumnSpec]]):
        return QuestionInstrumentItemSpec(
            prompt=fake.text(20),
            type='question',
            remote_id=self.remote_id(),
            id=None if connect_node is None else connect_node[0],
            map={},
        )

    def constant_instrument_node(self, connect_node: t.Tuple[str, MeasureNodeSpec | IndexColumnSpec]):
        return ConstantInstrumentItemSpec(
            type='constant',
            value=fake.word(),
            id=connect_node[0],
        )

    def remote_id(self):
        return 'remote_{}'.format(fake.unique.word())

    def instrument_node_tree(self, pool: t.Collection[InstrumentNodeSpec], n: int = 6, depth: int = 2) -> t.Tuple[InstrumentNodeSpec, ...]:
        if len(pool) < n:
            return ()

        choice_items: t.Collection[InstrumentNodeSpec] = self.parent.random_elements(pool, length=n, unique=True) # type: ignore
        remaining_items = tuple(i for i in pool if i not in choice_items)
        branch = [
            InstrumentItemGroupSpec(
                type='group',
                prompt=fake.text(20),
                title=fake.text(20),
                items=self.instrument_node_tree(remaining_items, n, depth-1),
            )
        ] if depth > 0 else []
        return (*branch, *choice_items)


class StudySpecProvider(BaseProvider):

    def study_spec(self) -> StudySpec:
        config=self.study_config()
        measures=self.measure_map()
        context=InstrumentCreationContext(
            self,
            make_measure_node_pool(measures),
            make_index_column_pool(config.indices),
        )
        return StudySpec(
            config=config,
            measures=measures,
            instruments=context.instrument_map(),
        )

    def measure_map(self, n: int = 4) -> t.Mapping[str, MeasureSpec]:
        return {
            self.measure_name(): self.measure()
                for _ in range(n)
        }

    def measure_name(self) -> str:
        return "measure_{}".format(fake.unique.word())

    def measure(self) -> MeasureSpec:
        codes = self.codemap_map()

        ordinal = tuple(self.ordinal_measure_item(tuple(codes)) for _ in range(30))
        simple = tuple(self.simple_measure_item() for _ in range(30))

        pool = ordinal + simple

        return MeasureSpec(
            title=fake.text(20),
            description=fake.sentence(20),
            items=self.measure_node_tree(pool),
            codes=codes,
        )

    def measure_node_tree(self, pool: t.Collection[MeasureNodeSpec], n: int = 6, depth: int = 2) -> t.Tuple[MeasureNodeSpec, ...]:
        if len(pool) < n:
            return ()

        choice_items: t.Collection[MeasureNodeSpec] = self.random_elements(pool, length=n, unique=True) # type: ignore
        remaining_items = tuple(i for i in pool if i not in choice_items)
        branch = [MeasureItemGroupSpec(
            id=self.measure_node_name(),
            prompt=fake.text(20),
            type='group',
            items=self.measure_node_tree(remaining_items, n, depth-1)
        )] if depth > 0 else []
        return (*branch, *choice_items)

    def measure_node_name(self) -> RelativeMeasureNodeName:
        return RelativeMeasureNodeName("m_{}".format(fake.unique.word()))

    def simple_measure_item(self) -> SimpleMeasureItemSpec:
        return SimpleMeasureItemSpec(
            id=self.measure_node_name(),
            prompt=fake.text(20),
            type=self.random_element(('text', 'real', 'integer')), # type: ignore
        )

    def ordinal_measure_item(self, codes: t.Collection[RelativeCodeMapName]) -> OrdinalMeasureItemSpec:
        return OrdinalMeasureItemSpec(
            id=self.measure_node_name(),
            prompt=fake.text(20),
            type=self.random_element(('ordinal', 'categorical')), # type: ignore
            codes=self.random_element(codes), # type: ignore
        )

    def study_config(self) -> StudyConfigSpec:
        return StudyConfigSpec(
            title=fake.text(30),
            description=fake.sentence(),
            indices=self.index_column_map(),
        )

    def index_column_map(self, n: int = 4) -> t.Mapping[RelativeIndexColumnName, IndexColumnSpec]:
        return {
            self.index_column_name(): self.index_column()
                for _ in range(n)
        }

    def index_column_name(self) -> RelativeIndexColumnName:
        return RelativeIndexColumnName("index_{}".format(fake.unique.word()))

    def index_column(self) -> IndexColumnSpec:
        return IndexColumnSpec(
            title=fake.text(20),
            description=fake.sentence(20),
            values=self.codemap(),
        )

    def codemap_name(self):
        return RelativeCodeMapName("codemap_{}".format(fake.unique.word()))

    def codemap_map(self, n: int = 4):
        return {
            self.codemap_name(): self.codemap()
                for _ in range(n)
        }

    def codemap(self, n: int = 4):
        values = tuple(OrdinalValue(i) for i in range(1, n))
        tags = tuple(fake.unique.word().upper() for _ in values)
        texts = tuple(fake.sentence(3) for _ in values)
        return CodeMapSpec(
            __root__=tuple(
                CodeMapSpec.Value(
                    value=value,
                    tag=tag,
                    text=text
                ) for value, tag, text in zip(values, tags, texts)
            )
        )