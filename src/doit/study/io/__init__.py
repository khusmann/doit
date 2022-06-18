import typing as t

from ..spec import (
    StudySpec,
    MeasureSpec,
    InstrumentSpec,
    StudyConfigSpec,
)

T = t.TypeVar('T')

def helper(f: t.Callable[[], T], thing: str) -> T:
    try:
        return f()
    except Exception as e:
        raise Exception("Error parsing {}.yaml, {}".format(thing, e))

def load_studyspec_str(
    config: str,
    measures: t.Mapping[str, str],
    instruments: t.Mapping[str, str],
    parser: t.Callable[[str], t.Any],
):
    return StudySpec(
        config=StudyConfigSpec.parse_obj(parser(config)),
        measures={ k: helper(lambda: MeasureSpec.parse_obj(parser(v)), k) for k, v in measures.items() },
        instruments={ k: helper(lambda: InstrumentSpec.parse_obj(parser(v)), k) for k, v in instruments.items() }
    )