from __future__ import annotations
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

class QualtricsMultiselectQuestionType2(ImmutableBaseModel):
    type: t.Literal['Matrix']
    selector: t.Literal['Likert']
    subSelector: t.Literal['DL']

class QualtricsOrdinalGroupQuestionType(ImmutableBaseModel):
    type: t.Literal['Matrix']
    selector: t.Literal['Likert']
    subSelector: t.Literal['SingleAnswer', 'MultipleAnswer']

class QualtricsGroupQuestionType2(ImmutableBaseModel):
    type: t.Literal['Matrix']
    selector: t.Literal['CS']
    subSelector: t.Literal['WVTB']

class QualtricsGroupQuestionType3(ImmutableBaseModel):
    type: t.Literal['Matrix']
    selector: t.Literal['TE']
    subSelector: t.Literal['Short']

class QualtricsSuperGroupQuestionType(ImmutableBaseModel):
    type: t.Literal['SBS']
    selector: t.Literal['SBSMatrix']

class QualtricsSubquestion(ImmutableBaseModel):
    choiceText: str

class QualtricsCodes(ImmutableBaseModel):
    choiceText: str

class QualtricsSimpleQuestion(ImmutableBaseModel):
    questionType: t.Union[QualtricsHeaderQuestionType, QualtricsTextQuestionType]
    questionText: str

class QualtricsCodedQuestion(ImmutableBaseModel):
    questionType: t.Union[QualtricsOrdinalQuestionType, QualtricsMultiselectQuestionType, QualtricsMultiselectQuestionType2]
    questionText: str
    choices: t.Optional[t.Mapping[str, QualtricsCodes]]

class QualtricsGroupQuestion(ImmutableBaseModel):
    questionType: t.Union[QualtricsOrdinalGroupQuestionType, QualtricsGroupQuestionType2, QualtricsGroupQuestionType3, QualtricsMultiselectQuestionType2]
    questionText: str
    subQuestions: t.Mapping[str, QualtricsSubquestion]
    choices: t.Mapping[str, QualtricsCodes]

class QualtricsSuperGroupQuestion(ImmutableBaseModel):
    questionType: QualtricsSuperGroupQuestionType
    questionText: str
    columns: t.Mapping[str, QualtricsQuestion]

QualtricsQuestion = t.Union[
    QualtricsSimpleQuestion,
    QualtricsGroupQuestion,
    QualtricsSuperGroupQuestion,
    QualtricsCodedQuestion,
]

QualtricsSuperGroupQuestion.update_forward_refs()

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
                        remote_id=export_tags[qid+"_TEXT"],
                        id=None,
                    )
                case QualtricsHeaderQuestionType():
                    return QuestionInstrumentItemSpec(
                        prompt=prompt,
                        type='question',
                        remote_id=None,
                        id=None,
                    )
        case QualtricsSuperGroupQuestion():
            return InstrumentItemGroupSpec(
                prompt=prompt,
                type='group',
                items=tuple(convert_question(qid+"#"+key+"_4", c, export_tags) for key, c in qualtrics_question.columns.items())
            )
        case QualtricsGroupQuestion():
            match qualtrics_question.questionType:
                case QualtricsOrdinalGroupQuestionType() | QualtricsMultiselectQuestionType2():
                    postfix=""
                case QualtricsGroupQuestionType2():
                    postfix="_1"
                case QualtricsGroupQuestionType3():
                    postfix="_4"
            return InstrumentItemGroupSpec(
                prompt=prompt,
                type='group',
                items=tuple(
                    QuestionInstrumentItemSpec(
                        prompt=extract_text(i.choiceText),
                        type='question',
                        remote_id=export_tags["{}_{}{}".format(qid, sub_id, postfix)],
                        id=None,
                        map={ extract_text(c.choiceText): None for c in qualtrics_question.choices.values() },
                    ) for sub_id, i in qualtrics_question.subQuestions.items()),
            )
        case QualtricsCodedQuestion():
            if qualtrics_question.choices:
                return QuestionInstrumentItemSpec(
                    prompt=prompt,
                    type='question',
                    remote_id=export_tags[qid],
                    id=None,
                    map={ extract_text(c.choiceText): None for c in qualtrics_question.choices.values() },
                )
            else:
                return QuestionInstrumentItemSpec(
                    prompt=prompt,
                    type='question',
                    remote_id=None,
                    id=None,
                )

from ...unsanitizedtable.io.qualtrics import QualtricsSchema

def stub_instrumentspec_qualtrics(survey_json: str, schema_json: str) -> InstrumentSpec:
    survey = QualtricsSurvey.parse_raw(survey_json)

    question_list = tuple(
        e.questionId 
            for b in survey.blocks.values()
                for e in b.elements
                    if e.type == 'Question'
    )

    qs = QualtricsSchema.parse_raw(schema_json)

    export_tags = { key: value.exportTag for key, value in qs.properties.values.properties.items() }

    return InstrumentSpec(
        title=survey.name,
        description="enter description here",
        instructions="enter instructions here",
        items=tuple(convert_question(i, survey.questions[i], export_tags) for i in question_list),
    )

def stub_instrumentspec_from_qualtrics_blob(filename: Path | str):
    import tarfile
    with tarfile.open(filename, 'r:gz') as tf:
        survey = tf.getmember('survey.json')
        survey_data = tf.extractfile(survey)
        schema = tf.getmember('schema.json')
        schema_data = tf.extractfile(schema)
        if not survey_data or not schema_data:
            raise Exception("Error: no data in survey.json")
        return stub_instrumentspec_qualtrics(survey_data.read().decode('utf-8'), schema_data.read().decode('utf-8'))