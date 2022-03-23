from __future__ import annotations
from pathlib import Path
from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from ...domain.value.common import InstrumentId, merge_mappings

from ...domain.value import pages

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

class StudyRepoReader:
    def __init__(self, path: Path):
        assert path.exists()
        self.url = "sqlite:///{}".format(path)
        print(self.url)
        self.engine = create_engine(self.url, echo=True)

    def query_instrument_listing(self) -> t.Tuple[pages.InstrumentListing, ...]:
        session = Session(self.engine)
        return tuple(map(pages.InstrumentListing.from_orm, session.query(Instrument).all()))

    def query_instrument(self, instrument_id: InstrumentId) -> pages.Instrument:
        session = Session(self.engine)

        instrument: t.List[Instrument] = list(session.query(Instrument).where(Instrument.tag == instrument_id)) # type: ignore
        assert(len(instrument) == 1)

        return pages.Instrument.from_orm(instrument[0])

    def query_measure_listing(self) -> t.Tuple[pages.MeasureListing, ...]:
        session = Session(self.engine)
        return tuple(map(pages.MeasureListing.from_orm, session.query(Measure).all()))

    def query_measure(self, measure_id: MeasureId) -> pages.Measure:
        session = Session(self.engine)

        measure: t.List[Measure] = list(session.query(Measure).where(Measure.tag == measure_id)) # type: ignore
        assert(len(measure) == 1)

        return pages.Measure.from_orm(measure[0])