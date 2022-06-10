import typing as t
import re
import hashlib

from ...common import ImmutableBaseModel

from pydantic import Field

from collections import abc

from ...common.table import (
    Omitted,
    TableRowView,
    cast_fn_seq,
    cast_fn,
    Some,
)

from ..model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedCodedColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableData,
    UnsanitizedSimpleColumnInfo,
)


### QualtricsSchema

class QualtricsCategoryItem(ImmutableBaseModel):
    label: str
    const: int

class QualtricsOrdinalQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    oneOf: t.List[QualtricsCategoryItem]
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsNumericQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsStringQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    type: t.Literal['string']
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsArrayQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    dataType: t.Literal['question', 'metadata', 'embeddedData']
    type: t.Literal['array']

class QualtricsOrdinalArrayItems(ImmutableBaseModel):
    oneOf: t.List[QualtricsCategoryItem]

class QualtricsOrdinalArrayQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    dataType: t.Literal['question', 'metadata', 'embeddedData']
    type: t.Literal['array']
    items: QualtricsOrdinalArrayItems

QualtricsQuestionSchema = t.Annotated[
    t.Union[
        QualtricsOrdinalQuestion,
        QualtricsNumericQuestion,
        QualtricsStringQuestion,
        QualtricsOrdinalArrayQuestion,
        QualtricsArrayQuestion
    ],
    Field(discriminator='type')
]

class QualtricsSchemaContentValues(ImmutableBaseModel):
    properties: t.Mapping[str, QualtricsQuestionSchema]

class QualtricsSchemaContent(ImmutableBaseModel):
    values: QualtricsSchemaContentValues

class QualtricsSchema(ImmutableBaseModel):
    title: str
    properties: QualtricsSchemaContent

### QualtricsData

class QualtricsDataRow(ImmutableBaseModel):
    responseId: str
    values: t.Mapping[str, t.Union[str, t.List[str]]]

class QualtricsData(ImmutableBaseModel):
    responses: t.List[QualtricsDataRow]

class QualtricsSchemaMapping(t.NamedTuple):
    qualtrics_ids: t.Tuple[str, ...]
    columns: t.Tuple[UnsanitizedColumnInfo, ...]

IGNORE_ITEMS = [
    "locationLongitude",
    "recipientFirstName",
    "recipientLastName",
    "ipAddress",
    "recipientEmail",
    "locationLatitude",
    "externalDataReference",
    ".*_DO",
]

def unsanitizedcolumninfo_from_qualtrics(key: str, value: QualtricsQuestionSchema, column_sort_order: t.Mapping[str, str]) -> UnsanitizedColumnInfo:
    id = UnsanitizedColumnId(value.exportTag if value.dataType == 'question' else key)
    key_parts = key.split("_")
    sortkey = "_".join((column_sort_order.get(key_parts[0], "0"), *key_parts[1:]))
    match value:
        case QualtricsStringQuestion(description=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=id,
                prompt=prompt,
                is_safe=False,
                sortkey=sortkey,
            )
        case QualtricsNumericQuestion(description=prompt):
            return UnsanitizedSimpleColumnInfo(
                id=id,
                prompt=prompt,
                is_safe=False,
                sortkey=sortkey,
            )
        case QualtricsOrdinalArrayQuestion(description=prompt,items=items):
            return UnsanitizedCodedColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in items.oneOf },
                value_type='multiselect',
                sortkey=sortkey,
            )
        case QualtricsOrdinalQuestion(description=prompt,oneOf=oneOf):
            return UnsanitizedCodedColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in oneOf },
                value_type='ordinal',
                sortkey=sortkey,
            )
        case QualtricsArrayQuestion(description=prompt):
            raise Exception("Not implemented: {}".format(value))

def parse_qualtrics_schema(qs: QualtricsSchema, column_sort_order: t.Mapping[str, str]) -> QualtricsSchemaMapping:
    responseId = (
        "responseId",
        UnsanitizedSimpleColumnInfo(
            id=UnsanitizedColumnId('responseId'),
            prompt="Qualtrics response id",
            is_safe=True,
            sortkey="0",
        )
    )
    responses = (
        (key, unsanitizedcolumninfo_from_qualtrics(key, value, column_sort_order))
            for key, value in qs.properties.values.properties.items()
                if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS)) and not isinstance(value, QualtricsArrayQuestion)
    )

    arrayQs = tuple(value for key, value in qs.properties.values.properties.items() if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS)) and isinstance(value, QualtricsArrayQuestion))

    if arrayQs:
        print("Warning, ignoring array type: {}".format(arrayQs))

    qmapping = (responseId, *responses)
    return QualtricsSchemaMapping(
        tuple(qid for qid, _ in qmapping),
        tuple(c for _, c in qmapping)
    )

def from_qualtrics_value(column: UnsanitizedColumnInfo, value: str | t.Sequence[str] | None):
    # TODO: encode missing values for unasked questions (due to branching, etc) as NotAsked() or something
    match value:
        case None | "":
            return Omitted()

        case str():
            match column.value_type:
                case 'text':
                    return Some(value)
                case 'ordinal':
                    return Some(value).bind(cast_fn(int))
                case 'multiselect':
                    raise Exception("Error: expected multiselect value, instead got {}".format(value))

        case abc.Sequence():
            if column.value_type != 'multiselect':
                raise Exception("Error: expected multiselect column type, instead got {}".format(column))

            return Some(value).bind(cast_fn_seq(int))


def parse_qualtrics_data(
    schema_mapping: QualtricsSchemaMapping,
    qd: QualtricsData
) -> UnsanitizedTableData:


    rows = tuple(
        TableRowView(
            (column.id, (Some(row.responseId)) if name == 'responseId' else from_qualtrics_value(column, row.values.get(name)))
                for name, column in zip(*schema_mapping)
        ) for row in qd.responses
    )

    return UnsanitizedTableData(
        column_ids=tuple(c.id for c in schema_mapping.columns),
        rows=rows,
    )

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
    blocks: t.Mapping[str, QualtricsBlock]

def load_unsanitizedtable_qualtrics(schema_json: str, data_json: str, survey_json: str) -> UnsanitizedTable:
    qs = QualtricsSchema.parse_raw(schema_json)
    qd = QualtricsData.parse_raw(data_json)
    qsurvey = QualtricsSurvey.parse_raw(survey_json)

    ordered_question_list = tuple(
        e 
            for b in qsurvey.blocks.values()
                for e in b.elements
                    if e.type == 'Question'
    )

    column_sort_order = { qid.questionId: str(i).zfill(6) for i, qid in enumerate(ordered_question_list)}

    schema_map = parse_qualtrics_schema(qs, column_sort_order)

    return UnsanitizedTable(
        schema=schema_map.columns,
        data=parse_qualtrics_data(schema_map, qd),
        schema_checksum=hashlib.sha256(schema_json.encode()).hexdigest(),
        data_checksum=hashlib.sha256(data_json.encode()).hexdigest(),
        source_name='qualtrics',
        source_title=qs.title,
    )

