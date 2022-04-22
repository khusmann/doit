import typing as t
import re
import hashlib
from pydantic import Field, BaseModel

from doit.common import (
    TableRowView,
    omitted_if_empty,
)

from ..model import (
    UnsanitizedArrayColumnInfo,
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedOrdinalColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableData,
    UnsanitizedTableSchema,
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
    id = UnsanitizedColumnId(value.exportTag if value.dataType == 'question' else "qualtrics_" + key)
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

def load_unsanitizedtableschema_qualtrics(schema_json: str) -> UnsanitizedTableSchema:
    qs = QualtricsSchema.parse_raw(schema_json)
    return UnsanitizedTableSchema(
        columns=tuple(
            unsanitizedcolumninfo_from_qualtrics(key, value)
                for key, value in qs.properties.values.properties.items()
                    if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS))
        )
    )


def load_unsanitizedtabledata_qualtrics(column_ids: t.Tuple[UnsanitizedColumnId], data_json: str) -> UnsanitizedTableData:
    qd = QualtricsData.parse_raw(data_json)

    return UnsanitizedTableData(
        column_ids=column_ids,
        rows=tuple(
            TableRowView({
                cid: omitted_if_empty(row.values.get(cid.unsafe_name))
                    for cid in column_ids
            }) for row in qd.responses
        ),
    )

def load_unsanitizedtable_qualtrics(schema_json: str, data_json: str) -> UnsanitizedTable:
    schema = load_unsanitizedtableschema_qualtrics(schema_json)
    return UnsanitizedTable(
        schema=schema,
        data=load_unsanitizedtabledata_qualtrics(tuple(c.id for c in schema.columns), data_json),
        schema_checksum=hashlib.sha256(schema_json.encode()).hexdigest(),
        data_checksum=hashlib.sha256(data_json.encode()).hexdigest(),
    )

