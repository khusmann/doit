import typing as t

from ..spec import (
    StudySpec,
    MeasureSpec,
    InstrumentSpec,
    StudyConfigSpec,
)

# TODO: Better error messages

def load_studyspec_str(
    config: str,
    measures: t.Mapping[str, str],
    instruments: t.Mapping[str, str],
    parser: t.Callable[[str], t.Any],
):
    loaded_instruments: t.Mapping[str, InstrumentSpec] = {}
    for k, v in instruments.items():
        try:
            loaded_instruments |= { k: InstrumentSpec.parse_obj(parser(v)) }
        except Exception as e:
            raise Exception("Error parsing {}; {}".format(k, e))
    return StudySpec(
        config=StudyConfigSpec.parse_obj(parser(config)),
        measures={ k: MeasureSpec.parse_obj(parser(v)) for k, v in measures.items() },
        instruments=loaded_instruments,
    )