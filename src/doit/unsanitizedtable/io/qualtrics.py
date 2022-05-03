import typing as t
import re
import hashlib

from ...common import ImmutableBaseModel

from pydantic import Field

from doit.common.table import (
    Omitted,
    TableRowView,
    from_optional,
    Some,
)

from ..model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedOrdinalColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableData,
    UnsanitizedTextColumnInfo,
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
            return UnsanitizedOrdinalColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in items.oneOf },
                value_type='multiselect',
            )
        case QualtricsOrdinalQuestion(description=prompt,oneOf=oneOf):
            return UnsanitizedOrdinalColumnInfo(
                id=id,
                prompt=prompt,
                codes={ i.const: i.label for i in oneOf },
                value_type='ordinal',
            )
        case QualtricsArrayQuestion(description=prompt):
            raise Exception("Not implemented: {}".format(value))

def parse_qualtrics_schema(qs: QualtricsSchema) -> QualtricsSchemaMapping:
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

def from_qualtrics_value(column: UnsanitizedColumnInfo, value: t.Any):
    # TODO: encode missing values for unasked questions (due to branching, etc) as NotAsked() or something
    match column:
        case UnsanitizedTextColumnInfo():
            return from_optional(value, Omitted())
        case UnsanitizedOrdinalColumnInfo():
            return from_optional(value, Omitted()).bind(lambda x: Some(int(x)), str)

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

def load_unsanitizedtable_qualtrics(schema_json: str, data_json: str) -> UnsanitizedTable:
    qs = QualtricsSchema.parse_raw(schema_json)
    qd = QualtricsData.parse_raw(data_json)
    schema_map = parse_qualtrics_schema(qs)
    return UnsanitizedTable(
        schema=schema_map.columns,
        data=parse_qualtrics_data(schema_map, qd),
        schema_checksum=hashlib.sha256(schema_json.encode()).hexdigest(),
        data_checksum=hashlib.sha256(data_json.encode()).hexdigest(),
        source_name='qualtrics',
        source_title=qs.title,
    )

