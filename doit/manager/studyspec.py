import typing as t
#from time import time
from .settings import ProjectSettings
import yaml
from ..domain.value import (
    InstrumentSpec,
    MeasureSpec,
    StudySpec,
    InstrumentName,
    MeasureName,
    StudyConfigSpec,
    ImmutableBaseModel,
)

class StudySpecManager(ImmutableBaseModel):
    settings = ProjectSettings()

    def load_study_spec(self) -> StudySpec:
        instruments = { i: self.load_instrument_spec(i) for i in self.instruments }
        measures = { i: self.load_measure_spec(i) for i in self.measures }
        return StudySpec(
            config=self.load_config_spec(),
            measures=measures,
            instruments=instruments,
        )

    def load_config_spec(self) -> StudyConfigSpec:
        with open(self.settings.config_file, 'r') as f:
            return StudyConfigSpec.parse_obj(
                yaml.safe_load(f)
            )

    def load_instrument_spec(self, instrument_id: InstrumentName) -> InstrumentSpec:
        with open(self.settings.instrument_file(instrument_id), 'r') as f:
            return InstrumentSpec.parse_obj({
                "instrument_id": instrument_id,
                **yaml.safe_load(f)
            })

    def load_measure_spec(self, measure_id: MeasureName) -> MeasureSpec:
        with open(self.settings.measure_file(measure_id), 'r') as f:
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
        return [ InstrumentName(i.stem) for i in self.settings.instrument_dir.glob("*.yaml")]

    @property
    def measures(self) -> t.Sequence[MeasureName]:
        return [ MeasureName(i.stem) for i in self.settings.measure_dir.glob("*.yaml")]