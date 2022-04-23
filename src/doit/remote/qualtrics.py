from __future__ import annotations
import typing as t
from pathlib import Path
import requests
import json
import zipfile
import io
from pydantic import BaseModel, BaseSettings, Field, parse_obj_as

from . import RemoteTableListing

class QualtricsRemoteSettings(BaseSettings):
    api_key: t.Optional[str]
    data_center: t.Optional[str]
    api_url = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    class Config(BaseSettings.Config):
        env_prefix = "qualtrics_"

def fetch_qualtrics_source(remote_id: str, save_data_path: Path, save_schema_path: Path, settings: QualtricsRemoteSettings = QualtricsRemoteSettings()):
    return QualtricsRemote(settings).fetch_remote_table(remote_id, save_data_path, save_schema_path)

def fetch_qualtrics_listing(settings: QualtricsRemoteSettings = QualtricsRemoteSettings()):
    return QualtricsRemote(settings).fetch_table_listing()

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

QualtricsExportStatus = t.Annotated[
    t.Union[
        QualtricsExportStatusComplete,
        QualtricsExportStatusInProgress,
        QualtricsExportStatusFailed
    ], Field(discriminator='status')
]

class QualtricsRemote:
    settings = QualtricsRemoteSettings
    def __init__(self, settings: QualtricsRemoteSettings):
        self.settings = settings

    def get_endpoint_url(self, endpoint: str) -> str:
        return self.settings.api_url.format(data_center=self.settings.data_center, endpoint=endpoint)

    def get_headers(self) -> t.Mapping[str, str]:
        if not self.settings.api_key:
            raise Exception("Error: QUALTRICS_API_KEY not set. Try making an .env file.")
        return {
            "content-type": "application/json",
            "x-api-token": self.settings.api_key,
        }

    def get(self, endpoint: str, stream: bool = False) -> requests.Response:
        return requests.request("GET", self.get_endpoint_url(endpoint), headers=self.get_headers(), stream=stream)

    def post(self, endpoint: str, payload: t.Mapping[str, t.Any]) -> requests.Response:
        return requests.request("POST", self.get_endpoint_url(endpoint), data=json.dumps(payload), headers=self.get_headers())

    def fetch_table_listing(self) -> t.List[RemoteTableListing]:
        response = self.get("surveys").json()
        assert 'result' in response
        survey_list = QualtricsSurveyList(**response['result'])
        return [
            RemoteTableListing(
                uri = "qualtrics://{}".format(i.id),
                title = i.name
            ) for i in survey_list.elements
        ]

    def fetch_remote_table(
        self,
        remote_id: str,
        data_path: Path,
        schema_path: Path,
        progress_callback: t.Callable[[int], None] = lambda _: None,
    ):
        self.fetch_remote_table_data(remote_id, data_path, progress_callback)
        self.fetch_remote_table_schema(remote_id, schema_path)
        progress_callback(100)

    def fetch_remote_table_data(
        self,
        qualtrics_id: str,
        data_path: Path,
        progress_callback: t.Callable[[int], None] = lambda _: None,
    ) -> None:
        endpoint_prefix = "surveys/{}/export-responses".format(qualtrics_id)
        response = self.post(endpoint_prefix, dict(format='json')).json()
        
        assert 'result' in response
        progressId = QualtricsExportResponse(**response['result']).progressId

        progressStatus = QualtricsExportStatusInProgress(
            percentComplete = "0",
            status="inProgress"
        )

        while progressStatus.status != "complete" and progressStatus.status != "failed":
            progress_callback(int(float(progressStatus.percentComplete)))

            response = self.get("{}/{}".format(endpoint_prefix, progressId)).json()
            assert 'result' in response

            progressStatus = parse_obj_as(QualtricsExportStatus, response['result'])

        if progressStatus.status == "failed":
            raise Exception("export failed")

        requestDownload = self.get("{}/{}/file".format(endpoint_prefix, progressStatus.fileId), stream=True)

        with zipfile.ZipFile(io.BytesIO(requestDownload.content)) as zip:
            assert len(zip.filelist) == 1
            datafile = Path(zip.extract(zip.filelist[0], Path(data_path).parent))
            datafile.rename(data_path)

    def fetch_remote_table_schema(self, qualtrics_id: str, schema_path: Path) -> None:
        endpoint_prefix = "surveys/{}/response-schema".format(qualtrics_id)
        response = self.get(endpoint_prefix).json()
        assert 'result' in response

        with open(schema_path, 'w') as f:
            json.dump(response['result'], f)