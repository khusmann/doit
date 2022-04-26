import typing as t
import yaml


from .spec import (
    StudySpec,
    MeasureSpec,
    InstrumentSpec,
    StudyConfigSpec,
)

# TODO: Better error messages

def load_studyspec_yaml(
    config: str,
    measures: t.Mapping[str, str],
    instruments: t.Mapping[str, str],
):
    return StudySpec(
        config=StudyConfigSpec.parse_obj(yaml.safe_load(config)),
        measures={ k: MeasureSpec.parse_obj(yaml.safe_load(v)) for k, v in measures.items() },
        instruments={ k: InstrumentSpec.parse_obj(yaml.safe_load(v)) for k, v in instruments.items() },
    )