import typing as t
from time import time
from pydantic import BaseSettings
from pathlib import Path
import yaml
from ..domain.value import (
    ImmutableBaseModel,
    Instrument,
    Measure,
    Study,
    InstrumentId,
    MeasureId,
)

class StudyRepoSettings(BaseSettings):
    instrument_dir = Path("./instruments")
    measure_dir = Path("./measures")

    def instrument_file(self, instrument_id: InstrumentId) -> Path:
        return (self.instrument_dir / instrument_id).with_suffix(".yaml")

    def measure_file(self, measure_id: MeasureId) -> Path:
        return (self.measure_dir / measure_id).with_suffix(".yaml")


class StudyRepo(ImmutableBaseModel):
    settings = StudyRepoSettings()

    def query(self) -> Study:
        return Study(
            title="Study title",
            instruments={ i: self.query_instrument(i) for i in self.instruments },
            measures={ i: self.query_measure(i) for i in self.measures },
        )

    def query_instrument(self, instrument_id: InstrumentId) -> Instrument:
        with open(self.settings.instrument_file(instrument_id), 'r') as f:
            return Instrument.parse_obj({
                "instrument_id": instrument_id,
                **yaml.safe_load(f)
            })

    def query_measure(self, measure_id: MeasureId) -> Measure:
        with open(self.settings.measure_file(measure_id), 'r') as f:
            return Measure.parse_obj({
                "measure_id": measure_id,
                **yaml.safe_load(f)
            })

    def save_instrument(self, instrument: Instrument):
        file = self.settings.instrument_file(instrument.instrument_id)

        if file.exists():
            newfile = file.with_name("{}.{}".format(file.name, int(time())))
            file.rename(newfile)


        with open(file, 'w') as f:
            yaml.dump(instrument.dict(exclude={'instrument_id'}), f)

    @property
    def instruments(self) -> t.Sequence[InstrumentId]:
        return [ InstrumentId(i.stem) for i in self.settings.instrument_dir.glob("*.yaml")]

    @property
    def measures(self) -> t.Sequence[MeasureId]:
        return [ MeasureId(i.stem) for i in self.settings.measure_dir.glob("*.yaml")]