import typing as t
#from time import time
from pydantic import BaseSettings
from pathlib import Path
import yaml
from ..domain.value import (
    InstrumentSpec,
    MeasureSpec,
    StudySpec,
    InstrumentName,
    MeasureName,
    ConfigSpec,
)

class StudySpecManager(BaseSettings):
    instrument_dir = Path("./instruments")
    measure_dir = Path("./measures")
    config_file = Path("./config.yaml")

    def instrument_file(self, instrument_id: InstrumentName) -> Path:
        return (self.instrument_dir / instrument_id).with_suffix(".yaml")

    def measure_file(self, measure_id: MeasureName) -> Path:
        return (self.measure_dir / measure_id).with_suffix(".yaml")

    def load_study_spec(self) -> StudySpec:
        instruments = { i: self.load_instrument_spec(i) for i in self.instruments }
        measures = { i: self.load_measure_spec(i) for i in self.measures }
        return StudySpec(
            config=self.load_config_spec(),
            measures=measures,
            instruments=instruments,
        )

    def load_config_spec(self) -> ConfigSpec:
        with open(self.config_file, 'r') as f:
            return ConfigSpec.parse_obj(
                yaml.safe_load(f)
            )

    def load_instrument_spec(self, instrument_id: InstrumentName) -> InstrumentSpec:
        with open(self.instrument_file(instrument_id), 'r') as f:
            return InstrumentSpec.parse_obj({
                "instrument_id": instrument_id,
                **yaml.safe_load(f)
            })

    def load_measure_spec(self, measure_id: MeasureName) -> MeasureSpec:
        with open(self.measure_file(measure_id), 'r') as f:
            return MeasureSpec.parse_obj({
                "measure_id": measure_id,
                **yaml.safe_load(f)
            })

#    def save_instrument(self, instrument: Instrument):
#        file = self.instrument_file(instrument.instrument_id)
#
#        if file.exists():
#            newfile = file.with_name("{}.{}".format(file.name, int(time())))
#            file.rename(newfile)
#
#
#        with open(file, 'w') as f:
#            yaml.dump(instrument.dict(exclude={'instrument_id'}), f)

    @property
    def instruments(self) -> t.Sequence[InstrumentName]:
        return [ InstrumentName(i.stem) for i in self.instrument_dir.glob("*.yaml")]

    @property
    def measures(self) -> t.Sequence[MeasureName]:
        return [ MeasureName(i.stem) for i in self.measure_dir.glob("*.yaml")]