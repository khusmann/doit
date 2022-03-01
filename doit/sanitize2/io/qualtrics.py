from __future__ import annotations
import typing as t

from .instrumentsourceimpl import InstrumentSourceImpl, RemoteInstrumentDesc
import requests
import json
import re

from .instrumentsourceimpl import UnsafeTable, UnsafeDataColumn

from pathlib import Path
import zipfile
import io

from urllib.parse import urlparse

from pydantic import BaseModel, Field

from abc import ABC, abstractmethod

class QualtricsApi(ABC):
    @abstractmethod
    def get(self, endpoint: str, stream: bool = False) -> requests.Response:
        pass
    @abstractmethod
    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]) -> requests.Response:
        pass

    def uri_to_schema_filename(self, workdir: Path, uri: str):
        return (workdir / self.uri_to_qualtrics_id(uri)).with_suffix(".schema.json")

    def uri_to_data_filename(self, workdir: Path, uri: str):
        return (workdir / self.uri_to_qualtrics_id(uri)).with_suffix(".json")

    def uri_to_qualtrics_id(self, uri: str):
        result = urlparse(uri)
        assert result.scheme == "qualtrics"
        return result.netloc

class QualtricsApiImpl(QualtricsApi):
    api_key: str
    data_center: str
    API_URL = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    def __init__(self, api_key: str, data_center: str):
        self.api_key = api_key
        self.data_center = data_center

    def get_endpoint_url(self, endpoint: str):
        return QualtricsApiImpl.API_URL.format(data_center=self.data_center, endpoint=endpoint)

    def get_headers(self):
        return {
            "content-type": "application/json",
            "x-api-token": self.api_key,
        }

    def get(self, endpoint: str, stream: bool = False):
        return requests.request("GET", self.get_endpoint_url(endpoint), headers=self.get_headers(), stream=stream)

    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]):
        return requests.request("POST", self.get_endpoint_url(endpoint), data=json.dumps(payload), headers=self.get_headers())


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

### List Surveys API

class QualtricsSurveyListElement(BaseModel):
    id: str
    name: str

class QualtricsSurveyList(BaseModel):
    elements: t.List[QualtricsSurveyListElement]

### Download Survey API

class QualtricsExportResponse(BaseModel):
    progressId: str

class QualtricsExportStatusInProgress(BaseModel):
    percentComplete: str
    status: t.Literal["inProgress"]

class QualtricsExportStatusComplete(BaseModel):
    status: t.Literal["complete"]
    fileId: t.Optional[str]


class QualtricsExportStatusFailed(BaseModel):
    status: t.Literal["failed"]

class QualtricsExportStatus(BaseModel):
    __root__: t.Annotated[t.Union[
        QualtricsExportStatusComplete,
        QualtricsExportStatusInProgress,
        QualtricsExportStatusFailed
    ], Field(discriminator='status')]

### QualtricsSourceImpl

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

class QualtricsSourceImpl(InstrumentSourceImpl):
    def __init__(self, impl: QualtricsApi):
        self.impl = impl

    def fetch(self, workdir: Path, uri: str) -> None:
        print("Fetching... {} into {}".format(uri, workdir))
        self.fetch_schema(workdir, uri)
        self.fetch_data(workdir, uri)

    def load_data(self, workdir: Path, uri: str) -> UnsafeTable:
        qs = QualtricsSchema.parse_file(self.impl.uri_to_schema_filename(workdir, uri))

        valid_columns = {
            key: value for (key, value) in qs.properties.values.properties.items()
            if all(map(lambda i: not re.match(i, key), IGNORE_ITEMS))
        }

        data = QualtricsData.parse_file(self.impl.uri_to_data_filename(workdir, uri))

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

    def fetch_available_desc(self) -> t.List[RemoteInstrumentDesc]:
        response = self.impl.get("surveys").json()
        assert 'result' in response
        survey_list = QualtricsSurveyList(**response['result'])
        return [
            RemoteInstrumentDesc(
                uri = "qualtrics://{}".format(i.id),
                title = i.name
            ) for i in survey_list.elements
        ]

    def fetch_data(self, workdir: Path, uri: str):
        endpoint_prefix = "surveys/{}/export-responses".format(self.impl.uri_to_qualtrics_id(uri))
        response = self.impl.post(endpoint_prefix, dict(format='json')).json()
        
        assert 'result' in response
        progressId = QualtricsExportResponse(**response['result']).progressId

        progressStatus = QualtricsExportStatus.parse_obj(dict(percentComplete = "0%", status="inProgress")).__root__
        while progressStatus.status != "complete" and progressStatus.status != "failed":
            print ("progressStatus=", progressStatus.status)
            print("Download is " + progressStatus.percentComplete + " complete")

            response = self.impl.get("{}/{}".format(endpoint_prefix, progressId)).json()
            assert 'result' in response

            progressStatus = QualtricsExportStatus.parse_obj(response['result']).__root__

        if progressStatus.status == "failed":
            raise Exception("export failed")

        requestDownload = self.impl.get("{}/{}/file".format(endpoint_prefix, progressStatus.fileId), stream=True)

        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as zip:
            assert len(zip.filelist) == 1
            datafile = Path(zip.extract(zip.filelist[0], Path(self.impl.uri_to_data_filename(workdir, uri)).parent))
            datafile.rename(self.impl.uri_to_data_filename(workdir, uri))

    def fetch_schema(self, workdir: Path, uri: str):
        endpoint_prefix = "surveys/{}/response-schema".format(self.impl.uri_to_qualtrics_id(uri))
        response = self.impl.get(endpoint_prefix).json()
        assert 'result' in response

        with open(self.impl.uri_to_schema_filename(workdir, uri), 'w') as f:
            json.dump(response['result'], f)