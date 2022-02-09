import typing as t
import requests
import json

from pathlib import Path
import zipfile
import io

from urllib.parse import urlparse

from itertools import starmap

from abc import ABC, abstractmethod
from pyrsistent import PClass, field, pvector

from ..domain import downloadblob

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
    API_URL = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    def __init__(self, api_key: str, data_center: str):
        class QualtricsApiInfo(PClass):
            api_key = field(str)
            data_center = field(str)

        self.info = QualtricsApiInfo(api_key = api_key, data_center = data_center)

    def get_endpoint_url(self, endpoint: str):
        return QualtricsApiImpl.API_URL.format(data_center=self.info.data_center, endpoint=endpoint)

    def get_headers(self):
        return {
            "content-type": "application/json",
            "x-api-token": self.info.api_key,
        }

    def get(self, endpoint: str, stream: bool = False):
        return requests.request("GET", self.get_endpoint_url(endpoint), headers=self.get_headers(), stream=stream)

    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]):
        return requests.request("POST", self.get_endpoint_url(endpoint), data=json.dumps(payload), headers=self.get_headers())

class ListSurveysResultItem(PClass):
    id = field(str)
    name = field(str)

def toListSurveyResultItem(raw: t.Mapping[str, t.Any]):
    if 'id' in raw and 'name' in raw:
        return ListSurveysResultItem(id = raw['id'], name = raw['name'])
    else:
        raise Exception("Unexpected result item: " + str(raw))

def to_type_info(type: str, one_of: t.Sequence[t.Any] | None) -> downloadblob.TypeInfo:
    if one_of is None:
        return downloadblob.TypeInfo(
            dtype=type
        )
    else:
        return downloadblob.CategoryTypeInfo(
            categories={
                i['const']: i['label'] for i in one_of
            }
        )

def to_schema_entry(id: str, raw: t.Mapping[str, t.Any]):
    assert 'description' in raw
    assert 'type' in raw
    assert 'exportTag' in raw
    return downloadblob.SchemaEntry(
        id=id,
        rename_to=raw['exportTag'],
        type=to_type_info(raw['type'], raw.get('oneOf')),
        description=raw['description'],
    )


class QualtricsSchemaRepo(PClass):
    impl = field(QualtricsApi)
    
    def load(self, uri: str) -> downloadblob.Schema:
        with open(self.impl.uri_to_schema_filename(uri), 'r') as f:
            schema = json.load(f, parse_float=str, parse_int=str, parse_constant=str)
            assert 'title' in schema
            assert 'properties' in schema and 'values' in schema['properties'] and 'properties' in schema['properties']['values']
            return downloadblob.Schema(
                title=schema['title'],
                uri=uri,
                entries={ e.id: e for e in starmap(to_schema_entry, schema['properties']['values']['properties'].items()) }
            )

    def download(self, uri: str):
        endpoint_prefix = "surveys/{}/response-schema".format(self.impl.uri_to_qualtrics_id(uri))
        response = self.impl.get(endpoint_prefix).json()
        
        assert 'result' in response
        with open(self.impl.uri_to_schema_filename(uri), 'w') as f:
            json.dump(response['result'], f)

    def list_available(self):
        response = self.impl.get("surveys").json()
        if 'result' in response and 'elements' in response['result']:
            return pvector([toListSurveyResultItem(item) for item in response['result']['elements']])
        else:
            raise Exception("Result doesn't contain elements")



class QualtricsBlobRepo(PClass):
    impl = field(QualtricsApi)

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

