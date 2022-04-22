import typing as t
import re
import hashlib
from pydantic import Field, BaseModel

from doit.common import (
    TableRowView,
    omitted_if_empty,
    Some,
)

from ..model import (
    UnsanitizedArrayColumnInfo,
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedOrdinalColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableData,
    UnsanitizedTextColumnInfo,
)

### QualtricsSchema

class QualtricsCategoryItem(BaseModel):
    label: str
    const: str

class QualtricsOrdinalQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    oneOf: t.List[QualtricsCategoryItem]
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsNumericQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsStringQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['string']
    dataType: t.Literal['question', 'metadata', 'embeddedData']


class QualtricsArrayQuestion(BaseModel):
    description: str
    exportTag: str
    dataType: t.Literal['question', 'metadata', 'embeddedData']
    type: t.Literal['array']

class QualtricsOrdinalArrayItems(BaseModel):
    oneOf: t.List[QualtricsCategoryItem]

class QualtricsOrdinalArrayQuestion(BaseModel):
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

class QualtricsSchemaContentValues(BaseModel):
    properties: t.Mapping[str, QualtricsQuestionSchema]

class QualtricsSchemaContent(BaseModel):
    values: QualtricsSchemaContentValues

class QualtricsSchema(BaseModel):
    title: str
    properties: QualtricsSchemaContent

### QualtricsData

class QualtricsDataRow(BaseModel):
    responseId: str
    values: t.Mapping[str, t.Union[str, t.List[str]]]

class QualtricsData(BaseModel):
    responses: t.List[QualtricsDataRow]

## Functions

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

QUALTRICS_TYPE_MAP: t.Mapping[t.Type[QualtricsQuestionSchema], t.Literal['text', 'ordinal', 'bool', 'array']] = {
    QualtricsOrdinalQuestion: 'ordinal',
    QualtricsArrayQuestion: 'array',
    QualtricsNumericQuestion: 'text',
    QualtricsStringQuestion: 'text',
}

def unsanitizedcolumninfo_from_qualtrics(key: str, value: QualtricsQuestionSchema) -> UnsanitizedColumnInfo:
    id = UnsanitizedColumnId(value.exportTag if value.dataType == 'question' else key)
    match value:
        case QualtricsStringQuestion(description=prompt):
            return UnsanitizedTextColumnInfo(
                id=id,
                prompt=prompt,
                is_safe=False,
            )
        case QualtricsNumericQuestion(description=prompt):
            return UnsanitizedTextColumnInfo(
                id=id,
                prompt=prompt,
                is_safe=False,
            )
        case QualtricsOrdinalArrayQuestion(description=prompt,items=items):
            return UnsanitizedArrayColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in items.oneOf }
            )
        case QualtricsOrdinalQuestion(description=prompt,oneOf=oneOf):
            return UnsanitizedOrdinalColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in oneOf }
            )
        case QualtricsArrayQuestion(description=prompt):
            raise Exception("Not implemented: {}".format(value))

class QualtricsSchemaMapping(t.NamedTuple):
    qualtrics_ids: t.Tuple[str, ...]
    columns: t.Tuple[UnsanitizedColumnInfo, ...]

def load_qualtrics_schema_map(schema_json: str) -> QualtricsSchemaMapping:
    qs = QualtricsSchema.parse_raw(schema_json)
    responseId = (
        "responseId",
        UnsanitizedTextColumnInfo(
            id=UnsanitizedColumnId('responseId'),
            prompt="Qualtrics response id",
            is_safe=True,
        )
    )
    responses = (
        (key, unsanitizedcolumninfo_from_qualtrics(key, value))
            for key, value in qs.properties.values.properties.items()
                if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS))
    )
    qmapping = (responseId, *responses)
    return QualtricsSchemaMapping(
        tuple(qid for qid, _ in qmapping),
        tuple(c for _, c in qmapping)
    )

def load_unsanitizedtabledata_qualtrics(
    schema_mapping: QualtricsSchemaMapping,
    data_json: str
) -> UnsanitizedTableData:

    qd = QualtricsData.parse_raw(data_json)

    rows = tuple(
        TableRowView({
            column.id: (Some(row.responseId) if name == 'responseId' else omitted_if_empty(row.values.get(name)))
                for name, column in zip(*schema_mapping)
        }) for row in qd.responses
    )

    return UnsanitizedTableData(
        column_ids=tuple(c.id for c in schema_mapping.columns),
        rows=rows,
    )

def load_unsanitizedtable_qualtrics(schema_json: str, data_json: str) -> UnsanitizedTable:
    schema_map = load_qualtrics_schema_map(schema_json)
    return UnsanitizedTable(
        schema=schema_map.columns,
        data=load_unsanitizedtabledata_qualtrics(schema_map, data_json),
        schema_checksum=hashlib.sha256(schema_json.encode()).hexdigest(),
        data_checksum=hashlib.sha256(data_json.encode()).hexdigest(),
    )

