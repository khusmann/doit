#from re import A
import typing as t
import requests
import json

from pathlib import Path
import zipfile
import io

from abc import ABC, abstractmethod
from pyrsistent import PRecord, field, pvector

from .values import ListSurveysResultItem

class QualtricsApi(ABC):
    @abstractmethod
    def get(self, endpoint: str, stream: bool = False) -> requests.Response:
        pass
    @abstractmethod
    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]) -> requests.Response:
        pass

class QualtricsApiImpl(QualtricsApi):
    API_URL = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    def __init__(self, api_key: str, data_center: str):
        class QualtricsApiInfo(PRecord):
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

def toListSurveyResultItem(raw: t.Mapping[str, t.Any]):
    if 'id' in raw and 'name' in raw:
        return ListSurveysResultItem(id = raw['id'], name = raw['name'])
    else:
        raise Exception("Unexpected result item: " + str(raw))

def list_surveys(impl: QualtricsApi):
    response = impl.get("surveys").json()
    if 'result' in response and 'elements' in response['result']:
        return pvector([toListSurveyResultItem(item) for item in response['result']['elements']])
    else:
        raise Exception("Result doesn't contain elements")

def download_survey(impl: QualtricsApi, survey_id: str, save_filename: str):
    endpoint_prefix = "surveys/{}/export-responses".format(survey_id)
    response = impl.post(endpoint_prefix, dict(format='json')).json()
    
    assert 'result' in response and 'progressId' in response['result']
    progressId = response['result']['progressId']

    progressStatus = "inProgress"
    while progressStatus != "complete" and progressStatus != "failed":
        print ("progressStatus=", progressStatus)
        response = impl.get("{}/{}".format(endpoint_prefix, progressId)).json()
        assert 'result' in response and 'percentComplete' in response['result']
        requestCheckProgress = response["result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        assert 'result' in response and 'status' in response['result']
        progressStatus = response['result']['status']

    if progressStatus == "failed":
        raise Exception("export failed")

    assert 'result' in response and 'fileId' in response['result']
    file_id = response["result"]["fileId"]

    requestDownload = impl.get("{}/{}/file".format(endpoint_prefix, file_id), stream=True)

    with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as zip:
        assert len(zip.filelist) == 1
        datafile = Path(zip.extract(zip.filelist[0], Path(save_filename).parent))
        datafile.rename(save_filename)