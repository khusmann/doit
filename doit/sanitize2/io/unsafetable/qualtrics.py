from __future__ import annotations
import typing as t
import re
from pydantic import BaseModel, Field

from pathlib import Path

from .api import UnsafeTableIoApi
from ...domain.value import UnsafeTable, UnsafeDataColumn

class QualtricsUnsafeTableIo(UnsafeTableIoApi):
    def read_unsafe_table(self, data_path: Path, schema_path: Path) -> UnsafeTable:
        qs = QualtricsSchema.parse_file(schema_path)
        data = QualtricsData.parse_file(data_path)
        return extract_table(qs, data)

### QualtricsSchema

class QualtricsCategoryItem(BaseModel):
    label: str
    const: str

class QualtricsNumericQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['number']
    oneOf: t.Optional[t.List[QualtricsCategoryItem]]

class QualtricsStringQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['string']

class QualtricsArrayItems(BaseModel):
    oneOf: t.Optional[t.List[QualtricsCategoryItem]]

class QualtricsArrayQuestion(BaseModel):
    description: str
    exportTag: str
    type: t.Literal['array']
    items: QualtricsArrayItems


QualtricsQuestionSchema = t.Annotated[
    t.Union[QualtricsNumericQuestion,
            QualtricsStringQuestion,
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
def extract_responseId(rows: t.List[QualtricsDataRow]) -> UnsafeDataColumn:
    return UnsafeDataColumn(
        column_id = "responseId",
        prompt = "Qualtrics Response ID",
        type = "string",
        data = [row.responseId for row in rows]
    )

def extract_column(rows: t.List[QualtricsDataRow], row_key: str, schema: QualtricsQuestionSchema) -> t.List[UnsafeDataColumn]:
    raw_data = [row.values.get(row_key) for row in rows]
    list_data = [i for i in raw_data if i is None or isinstance(i, t.List)]
    str_data = [i for i in raw_data if i is None or isinstance(i, str)]

    match schema:
        case QualtricsNumericQuestion(oneOf=itemList) if itemList is not None:
            assert str_data == raw_data
            mapping = { i.const: i.label for i in itemList }

            return [UnsafeDataColumn(
                prompt = schema.description,
                column_id = schema.exportTag,
                type = "category",
                data = [None if i is None else mapping[i] for i in str_data],
            )]
        case QualtricsNumericQuestion():
            assert str_data == raw_data
            return [UnsafeDataColumn(
                prompt = schema.description,
                column_id = schema.exportTag,
                type = "numeric",
                data = str_data,
            )]
        case QualtricsStringQuestion():
            assert str_data == raw_data
            return [UnsafeDataColumn(
                prompt = schema.description,
                column_id = schema.exportTag,
                type = "string",
                data = str_data,
            )]
        case QualtricsArrayQuestion(items=items):
            assert list_data == raw_data
            assert items.oneOf is not None
            mapping = { i.const: i.label for i in items.oneOf }
            return [
                UnsafeDataColumn(
                    column_id = "{}_{}".format(schema.exportTag, i),
                    prompt = "{} {}".format(schema.description, opt.label),
                    type = "bool",
                    data = [None if i is None else opt.const in i for i in list_data],
                ) for (i, opt) in enumerate(items.oneOf)
            ]

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

def extract_table(qs: QualtricsSchema, data: QualtricsData) -> UnsafeTable:
    valid_columns = {
        key: value for (key, value) in qs.properties.values.properties.items()
        if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS))
    }
    data_extracted = [
        extract_responseId(data.responses),
        *sum([
            extract_column(data.responses, row_key, schema) for (row_key, schema) in valid_columns.items()
        ], [])
    ]

    return UnsafeTable(
        title = qs.title,
        columns = {
            i.column_id: i for i in data_extracted
        }
    )