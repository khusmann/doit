from __future__ import annotations
import typing as t
import re
from pydantic import Field

from pathlib import Path

from .api import UnsafeTableIoApi
from ..domain.value import ImmutableBaseModel, TableImport, ColumnImport

class QualtricsUnsafeTableIo(UnsafeTableIoApi):
    def read_unsafe_table_data(self, data_path: Path, schema_path: Path) -> TableImport:
        qs = QualtricsSchema.parse_file(schema_path)
        data = QualtricsData.parse_file(data_path)
        return extract_table_data(qs, data)

### QualtricsSchema

class QualtricsCategoryItem(ImmutableBaseModel):
    label: str
    const: str

class QualtricsNumericQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    oneOf: t.Optional[t.List[QualtricsCategoryItem]]
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsStringQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    type: t.Literal['string']
    dataType: t.Literal['question', 'metadata', 'embeddedData']

class QualtricsArrayItems(ImmutableBaseModel):
    oneOf: t.Optional[t.List[QualtricsCategoryItem]]

class QualtricsArrayQuestion(ImmutableBaseModel):
    description: str
    exportTag: str
    dataType: t.Literal['question', 'metadata', 'embeddedData']
    type: t.Literal['array']
    items: QualtricsArrayItems


QualtricsQuestionSchema = t.Annotated[
    t.Union[QualtricsNumericQuestion,
            QualtricsStringQuestion,
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


## Functions
def extract_responseId(rows: t.List[QualtricsDataRow]) -> ColumnImport:
    return ColumnImport(
        column_id = "responseId",
        prompt = "Qualtrics Response ID",
        type = "safe_text",
        values = [row.responseId for row in rows]
    )

def extract_column(rows: t.List[QualtricsDataRow], row_key: str, schema: QualtricsQuestionSchema) -> t.List[ColumnImport]:
    raw_data = [row.values.get(row_key) for row in rows]
    list_data = [i for i in raw_data if i is None or isinstance(i, t.List)]
    str_data = [i for i in raw_data if i is None or isinstance(i, str)]

    column_id = schema.exportTag if schema.dataType == 'question' else  "qualtrics_" + row_key

    if schema.type == 'array':
        assert list_data == raw_data
        assert schema.items.oneOf is not None
        mapping = { i.const: i.label for i in schema.items.oneOf }
        return [
            ColumnImport(
                column_id = "{}_{}".format(column_id, i),
                prompt = "{} {}".format(schema.description, opt.label),
                type = 'safe_bool',
                values = (None if i is None else opt.const in i for i in list_data),
            ) for (i, opt) in enumerate(schema.items.oneOf)
        ]

    match schema:
        case QualtricsNumericQuestion(oneOf=itemList) if itemList is not None:
            assert str_data == raw_data
            mapping = { i.const: i.label for i in itemList }
            column_type = 'safe_ordinal'
            values = (None if i is None else mapping[i] for i in str_data)
        case QualtricsNumericQuestion():
            assert str_data == raw_data
            column_type = "unsafe_numeric_text"
            values = str_data
        case QualtricsStringQuestion():
            assert str_data == raw_data
            column_type = "unsafe_text"
            values = str_data

    return [ColumnImport(
        column_id = column_id,
        prompt = schema.description,
        type = column_type,
        values = values,
    )]


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

def extract_table_data(qs: QualtricsSchema, data: QualtricsData) -> TableImport:
    valid_columns = {
        key: value for (key, value) in qs.properties.values.properties.items()
        if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS))
    }

    data_extracted = sorted(
        sum([
            extract_column(data.responses, row_key, schema) for (row_key, schema) in valid_columns.items()
        ], []),
        key=lambda i: i.column_id
    )

    return TableImport(
        title=qs.title,
        columns={ column.column_id: column for column in (extract_responseId(data.responses), *data_extracted) }
    )