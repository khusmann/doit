from ...common import ImmutableBaseModel
import typing as t
from ..spec import InstrumentItemGroupSpec, InstrumentNodeSpec, InstrumentSpec, QuestionInstrumentItemSpec
from pathlib import Path
import bs4

### QualtricsBlock

class QualtricsQuestionElement(ImmutableBaseModel):
    type: t.Literal['Question']
    questionId: str

class QualtricsPagebreakElement(ImmutableBaseModel):
    type: t.Literal['PageBreak']

QualtricsElement = t.Union[
    QualtricsQuestionElement,
    QualtricsPagebreakElement,
]

class QualtricsBlock(ImmutableBaseModel):
    elements: t.Tuple[QualtricsElement, ...]

### QualtricsQuestion

class QualtricsOrdinalQuestionType(ImmutableBaseModel):
    type: t.Literal['MC']
    selector: t.Literal['SAVR']
    subSelector: t.Literal['TX']

class QualtricsMultiselectQuestionType(ImmutableBaseModel):
    type: t.Literal['MC']
    selector: t.Literal['MAVR']
    subSelector: t.Literal['TX']

class QualtricsTextQuestionType(ImmutableBaseModel):
    type: t.Literal['TE']
    selector: t.Literal['SL', 'ML']
    subSelector: None

class QualtricsHeaderQuestionType(ImmutableBaseModel):
    type: t.Literal['DB']
    selector: t.Literal['TB']
    subSelector: None

class QualtricsOrdinalGroupQuestionType(ImmutableBaseModel):
    type: t.Literal['Matrix']
    selector: t.Literal['Likert']
    subSelector: t.Literal['SingleAnswer']

QualtricsQuestionType = t.Union[
    QualtricsOrdinalQuestionType,
    QualtricsTextQuestionType,
    QualtricsOrdinalGroupQuestionType,
    QualtricsMultiselectQuestionType,
    QualtricsHeaderQuestionType,
]

class QualtricsSubquestion(ImmutableBaseModel):
    choiceText: str

class QualtricsCodes(ImmutableBaseModel):
    choiceText: str

class QualtricsSimpleQuestion(ImmutableBaseModel):
    questionType: t.Union[QualtricsHeaderQuestionType, QualtricsTextQuestionType]
    questionText: str

class QualtricsCodedQuestion(ImmutableBaseModel):
    questionType: t.Union[QualtricsOrdinalQuestionType, QualtricsMultiselectQuestionType]
    questionText: str
    choices: t.Mapping[str, QualtricsCodes]

class QualtricsGroupQuestion(ImmutableBaseModel):
    questionType: QualtricsOrdinalGroupQuestionType
    questionText: str
    subQuestions: t.Mapping[str, QualtricsSubquestion]
    choices: t.Mapping[str, QualtricsCodes]

QualtricsQuestion = t.Union[
    QualtricsSimpleQuestion,
    QualtricsGroupQuestion,
    QualtricsCodedQuestion,
]

### Root Survey

class QualtricsExportColumn(ImmutableBaseModel):
    question: str
    subQuestion: t.Optional[str]

class QualtricsSurvey(ImmutableBaseModel):
    name: str
    exportColumnMap: t.Mapping[str, QualtricsExportColumn]
    questions: t.Mapping[str, QualtricsQuestion]
    blocks: t.Mapping[str, QualtricsBlock]

def extract_text(html: str):
    soup = bs4.BeautifulSoup("<!--{}-->".format(" "*256)+html, features="html.parser")
    return soup.get_text()

def convert_question(qid: str, qualtrics_question: QualtricsQuestion, export_tags: t.Mapping[str, str]) -> InstrumentNodeSpec:
    prompt = extract_text(qualtrics_question.questionText)
    match qualtrics_question:
        case QualtricsSimpleQuestion():
            match qualtrics_question.questionType:
                case QualtricsTextQuestionType():
                    return QuestionInstrumentItemSpec(
                        prompt=prompt,
                        type='question',
                        remote_id=export_tags[qid],
                        id=None,
                    )
                case QualtricsHeaderQuestionType():
                    return QuestionInstrumentItemSpec(
                        prompt=prompt,
                        type='question',
                        remote_id=None,
                        id=None,
                    )
        case QualtricsGroupQuestion():
            return InstrumentItemGroupSpec(
                prompt=prompt,
                type='group',
                items=tuple(
                    QuestionInstrumentItemSpec(
                        prompt=extract_text(i.choiceText),
                        type='question',
                        remote_id=export_tags["{}.subQuestions.{}".format(qid, sub_id)],
                        id=None,
                        map={ extract_text(c.choiceText): None for c in qualtrics_question.choices.values() },
                    ) for sub_id, i in qualtrics_question.subQuestions.items()),
            )
        case QualtricsCodedQuestion():
            return QuestionInstrumentItemSpec(
                prompt=prompt,
                type='question',
                remote_id=export_tags[qid],
                id=None,
                map={ extract_text(c.choiceText): None for c in qualtrics_question.choices.values() },
            )

def stub_instrumentspec_qualtrics(survey_json: str) -> InstrumentSpec:
    survey = QualtricsSurvey.parse_raw(survey_json)

    question_list = tuple(
        e.questionId 
            for b in survey.blocks.values()
                for e in b.elements
                    if e.type == 'Question'
    )

    export_tags = { (c.subQuestion if c.subQuestion else c.question):exportId for exportId, c in survey.exportColumnMap.items()}

    return InstrumentSpec(
        title=survey.name,
        description="enter description here",
        instructions="enter instructions here",
        items=tuple(convert_question(i, survey.questions[i], export_tags) for i in question_list),
    )

def stub_instrumentspec_from_qualtrics_blob(filename: Path | str):
    import tarfile
    with tarfile.open(filename, 'r:gz') as tf:
        member = tf.getmember('survey.json')
        data = tf.extractfile(member)
        if not data:
            raise Exception("Error: no data in survey.json")
        return stub_instrumentspec_qualtrics(data.read().decode('utf-8'))