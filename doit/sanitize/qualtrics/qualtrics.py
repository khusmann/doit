import typing as t
import requests
import json
import os
#import re
#import zipfile

from abc import ABC, abstractmethod
from pyrsistent import PRecord, field, pvector

#    r = re.compile('^SV_.*')
#    m = r.match(surveyId)
#    if not m:
#       print ("survey Id must match ^SV_.*")

class QualtricsApi(ABC):
    @abstractmethod
    def get(self, endpoint: str) -> t.Mapping[str, t.Any]:
        pass
    @abstractmethod
    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]):
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

    def get(self, endpoint: str):
        response = requests.request("GET", self.get_endpoint_url(endpoint), headers=self.get_headers())
        return response.json()
        
    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]):
        response = requests.request("POST", self.get_endpoint_url(endpoint), data=json.dumps(payload), headers=self.get_headers())
        return response.json()

def _get_env_qualtrics_api_impl():
    return QualtricsApiImpl(
        api_key=os.environ['QUALTRICS_API_KEY'],
        data_center=os.environ['QUALTRICS_DATA_CENTER'],
    )

def list_surveys():
   return _list_surveys(_get_env_qualtrics_api_impl())

class ListSurveysResultItem(PRecord):
    id = field(str)
    name = field(str)

def toListSurveyResultItem(raw: t.Mapping[str, t.Any]):
    if 'id' in raw and 'name' in raw:
        return ListSurveysResultItem(id = raw['id'], name = raw['name'])
    else:
        raise Exception("Unexpected result item: " + str(raw))

class ListSurveysResult():
    def __init__(self, response: t.Mapping[str, t.Any]):
        if 'result' in response and 'elements' in response['result']:
            self.items = pvector([toListSurveyResultItem(item) for item in response['result']['elements']])
        else:
            raise Exception("Result doesn't contain elements")

    def __str__(self):
        return "\n".join(["{}: {}".format(i.id, i.name) for i in self.items])


def _list_surveys(impl: QualtricsApi):
    return ListSurveysResult(impl.get("surveys"))


