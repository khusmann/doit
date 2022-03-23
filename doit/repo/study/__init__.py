from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from ...domain.value.common import merge_mappings

from .model import *

from ...domain.value import studyspec

from itertools import starmap
from functools import partial

from .converters import *

class StudyRepoWriter:
    def __init__(self, path: Path):
        assert not path.exists()
        self.url = "sqlite:///{}".format(path)
        print(self.url)


    def setup(self, study_spec: studyspec.StudySpec):
        # TODO: Add table meta...
        codemap_specs = tuple(map(lambda m: m.codes, study_spec.measures.values()))
        measure_item_specs = tuple(map(lambda m: m.items, study_spec.measures.values()))
        instrument_item_specs = tuple(map(lambda i: i.items, study_spec.instruments.values()))

        sql_measures = mapped_measurespec_to_sql(study_spec.measures)
        
        sql_codemaps = tuple(starmap(mapped_codemaps_to_sql, zip(codemap_specs, sql_measures.values())))
        
        sql_measure_nodes = merge_mappings(
            tuple(starmap(measure_node_tree_to_sql, zip(measure_item_specs, sql_measures.values(), sql_codemaps)))
        )

        sql_instruments = seq_instrumentspec_to_sql(tuple(study_spec.instruments.values()))

        sql_instrument_nodes = tuple(
            starmap(partial(instrument_node_tree_to_sql, measures=sql_measure_nodes), zip(instrument_item_specs, sql_instruments))
        ) # pyright: reportUnusedVariable=false
        
        self.engine = create_engine(self.url, echo=True)
        Base.metadata.create_all(self.engine)

        session = Session(self.engine)

        for measure in sql_measures.values():
            session.add(measure) # type: ignore

        for instrument in sql_instruments:
            session.add(instrument) # type: ignore

        session.commit()
