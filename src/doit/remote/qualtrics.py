from __future__ import annotations
import typing as t
import requests
import json
import zipfile
import io
from pydantic import BaseModel, BaseSettings, Field, parse_obj_as
from datetime import datetime, timezone

from .model import (
    LazyBlobData,
    RemoteTableListing,
    BlobInfo,
    SourceColumnInfo,
    QualtricsSourceInfo,
    Blob,
)

from ..unsanitizedtable.io.qualtrics import (
    load_unsanitizedtable_qualtrics
)

class QualtricsRemoteSettings(BaseSettings):
    api_key: t.Optional[str]
    data_center: t.Optional[str]
    api_url = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    class Config(BaseSettings.Config):
        env_prefix = "qualtrics_"

def fetch_qualtrics_listing(settings: QualtricsRemoteSettings = QualtricsRemoteSettings()):
    return QualtricsRemote(settings).fetch_table_listing()

def fetch_qualtrics_blob(remote_id: str, progress_callback: t.Callable[[int], None] = lambda _: None):
    remote = QualtricsRemote(QualtricsRemoteSettings())
    table_schema = remote.fetch_remote_table_schema(remote_id)
    table_data = remote.fetch_remote_table_data(remote_id, progress_callback)

    table = load_unsanitizedtable_qualtrics(table_schema, table_data)

    progress_callback(100)

    info = BlobInfo(
        fetch_date_utc=datetime.now(timezone.utc),
        title=table.source_title,
        source_info=QualtricsSourceInfo(
            type='qualtrics',
            remote_id=remote_id,
            data_checksum=table.data_checksum,
            schema_checksum=table.schema_checksum,
        ),
        columns=tuple(
            SourceColumnInfo(
                name=c.id.unsafe_name,
                prompt=c.prompt,
                ) for c in table.schema
        )
    )

    return Blob(
        info=info,
        data={
            "schema.json": table_schema.encode('utf-8'),
            "data.json": table_data.encode('utf-8'),
        }
    )

def load_qualtrics_blob_data(lazy_data: LazyBlobData):
    schema_lazy = lazy_data.get('schema.json')

    if not schema_lazy:
        raise Exception("Error: cannot find schema.json in qualtrics blob")

    data_lazy = lazy_data.get('data.json')

    if not data_lazy:
        raise Exception("Error: cannot find data.json in qualtrics blob")

    return load_unsanitizedtable_qualtrics(schema_lazy().decode('utf-8'), data_lazy().decode('utf-8'))

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

    def fetch_table_listing(self) -> t.Tuple[RemoteTableListing, ...]:
        response = self.get("surveys").json()
        assert 'result' in response
        survey_list = QualtricsSurveyList(**response['result'])
        return tuple(
            RemoteTableListing(
                uri = "qualtrics://{}".format(i.id),
                title = i.name
            ) for i in survey_list.elements
        )

    def fetch_remote_table_data(
        self,
        qualtrics_id: str,
        progress_callback: t.Callable[[int], None] = lambda _: None,
    ) -> str:
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
            return zip.read(zip.filelist[0]).decode(encoding="utf-8", errors='strict')

    def fetch_remote_table_schema(self, qualtrics_id: str) -> str:
        endpoint_prefix = "surveys/{}/response-schema".format(qualtrics_id)
        response = self.get(endpoint_prefix).json()
        assert 'result' in response
        return json.dumps(response['result'])