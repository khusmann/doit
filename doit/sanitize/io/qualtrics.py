import typing as t
import requests
import json

from pathlib import Path
import zipfile
import io

from urllib.parse import urlparse

from typing import OrderedDict, Optional
from pydantic import BaseModel

from abc import ABC, abstractmethod

from ..domain.schema import Schema, SchemaEntry, TypeInfo, CategoryTypeInfo

class QualtricsApi(ABC):
    @abstractmethod
    def get(self, endpoint: str, stream: bool = False) -> requests.Response:
        pass
    @abstractmethod
    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]) -> requests.Response:
        pass

    def uri_to_schema_filename(self, uri: str):
        return("./build/downloaded/{}.schema.json".format(self.uri_to_qualtrics_id(uri)))

    def uri_to_blob_filename(self, uri: str):
        return("./build/downloaded/{}.json".format(self.uri_to_qualtrics_id(uri)))

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

class ListSurveysResultItem(BaseModel):
    id: str
    name: str

def toListSurveyResultItem(raw: t.Mapping[str, t.Any]):
    if 'id' in raw and 'name' in raw:
        return ListSurveysResultItem(id = raw['id'], name = raw['name'])
    else:
        raise Exception("Unexpected result item: " + str(raw))

class QualtricsSchemaEntryOneOf(BaseModel):
    label: str
    const: str

class QualtricsSchemaEntry(BaseModel):
    type: str
    exportTag: str
    description: str
    oneOf: Optional[list[QualtricsSchemaEntryOneOf]]

class QualtricsSchema(BaseModel):
    title: str
    uri: str
    entries: OrderedDict[str, QualtricsSchemaEntry]

def to_schema_type(se: QualtricsSchemaEntry):
    match se:
        case QualtricsSchemaEntry(oneOf=oneOf) if oneOf is not None:
            return CategoryTypeInfo(
                categories=OrderedDict({
                    i.const: i.label for i in oneOf
                })
            )
        case _:
            return TypeInfo()

def to_schema(schema: QualtricsSchema):
    return Schema(
        title=schema.title,
        uri=schema.uri,
        entries=OrderedDict({
            variable_id: SchemaEntry(
                variable_id=variable_id,
                rename_to=p.exportTag,
                type=to_schema_type(p),
                description=p.description,
            ) for (variable_id, p) in sorted(schema.entries.items(), key=lambda i: i[0])
        })
    )

class QualtricsSchemaRepo:
    impl: QualtricsApi

    def __init__(self, impl: QualtricsApi):
        self.impl = impl
    
    def load(self, uri: str) -> Schema:
        with open(self.impl.uri_to_schema_filename(uri), 'r') as f:
            schema = json.load(f, parse_float=str, parse_int=str, parse_constant=str)
            assert 'title' in schema
            assert 'properties' in schema and 'values' in schema['properties'] and 'properties' in schema['properties']['values']
            return to_schema(QualtricsSchema(
                title=schema['title'],
                uri=uri,
                entries=schema['properties']['values']['properties']
            ))

    def download(self, uri: str):
        endpoint_prefix = "surveys/{}/response-schema".format(self.impl.uri_to_qualtrics_id(uri))
        response = self.impl.get(endpoint_prefix).json()
        
        assert 'result' in response
        with open(self.impl.uri_to_schema_filename(uri), 'w') as f:
            json.dump(response['result'], f)

    def list_available(self):
        response = self.impl.get("surveys").json()
        if 'result' in response and 'elements' in response['result']:
            return [toListSurveyResultItem(item) for item in response['result']['elements']]
        else:
            raise Exception("Result doesn't contain elements")



class QualtricsBlobRepo:
    def __init__(self, impl: QualtricsApi):
        self.impl = impl
 
#    def load(self, uri: str) -> downloadblob.Blob:
#        pass

    def download(self, uri: str):
        endpoint_prefix = "surveys/{}/export-responses".format(self.impl.uri_to_qualtrics_id(uri))
        response = self.impl.post(endpoint_prefix, dict(format='json')).json()
        
        assert 'result' in response and 'progressId' in response['result']
        progressId = response['result']['progressId']

        progressStatus = "inProgress"
        while progressStatus != "complete" and progressStatus != "failed":
            print ("progressStatus=", progressStatus)
            response = self.impl.get("{}/{}".format(endpoint_prefix, progressId)).json()
            assert 'result' in response and 'percentComplete' in response['result']
            requestCheckProgress = response["result"]["percentComplete"]
            print("Download is " + str(requestCheckProgress) + " complete")
            assert 'result' in response and 'status' in response['result']
            progressStatus = response['result']['status']

        if progressStatus == "failed":
            raise Exception("export failed")

        assert 'result' in response and 'fileId' in response['result']
        file_id = response["result"]["fileId"]

        requestDownload = self.impl.get("{}/{}/file".format(endpoint_prefix, file_id), stream=True)

        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as zip:
            assert len(zip.filelist) == 1
            datafile = Path(zip.extract(zip.filelist[0], Path(self.impl.uri_to_blob_filename(uri)).parent))
            datafile.rename(self.impl.uri_to_blob_filename(uri))

