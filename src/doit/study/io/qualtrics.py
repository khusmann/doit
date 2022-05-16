from ...common import ImmutableBaseModel
import typing as t
from ..spec import InstrumentSpec
from pathlib import Path

class QualtricsQuestion(ImmutableBaseModel):
    type: t.Literal['Question']
    questionId: str

class QualtricsPagebreak(ImmutableBaseModel):
    type: t.Literal['PageBreak']

QualtricsElement = t.Union[
    QualtricsQuestion,
    QualtricsPagebreak,
]

class QualtricsBlock(ImmutableBaseModel):
    elements: t.Tuple[QualtricsElement, ...]

class QualtricsSurvey(ImmutableBaseModel):
    name: str
    blocks: t.Mapping[str, QualtricsBlock]


def stub_instrumentspec_qualtrics(survey_json: str) -> InstrumentSpec:
    survey = QualtricsSurvey.parse_raw(survey_json)

    return InstrumentSpec(
        title=survey.name,
        description="enter description here",
        instructions="enter instructions here",
        items=(),
    )

def stub_instrumentspec_from_qualtrics_blob(filename: Path | str):
    import tarfile
    with tarfile.open(filename, 'r:gz') as tf:
        member = tf.getmember('survey.json')
        data = tf.extractfile(member)
        if not data:
            raise Exception("Error: no data in survey.json")
        return stub_instrumentspec_qualtrics(data.read().decode('utf-8'))