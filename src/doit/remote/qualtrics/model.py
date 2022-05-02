import typing as t
from ...common import ImmutableBaseModel
from pydantic import BaseSettings, Field

class QualtricsRemoteSettings(BaseSettings):
    api_key: t.Optional[str]
    data_center: t.Optional[str]
    api_url = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    class Config(BaseSettings.Config):
        env_prefix = "qualtrics_"

### List Surveys API
class QualtricsSurveyListElement(ImmutableBaseModel):
    id: str
    name: str

class QualtricsSurveyList(ImmutableBaseModel):
    elements: t.List[QualtricsSurveyListElement]

### Download Survey API

class QualtricsExportResponse(ImmutableBaseModel):
    progressId: str

class QualtricsExportStatusInProgress(ImmutableBaseModel):
    percentComplete: str
    status: t.Literal["inProgress"]

class QualtricsExportStatusComplete(ImmutableBaseModel):
    status: t.Literal["complete"]
    fileId: t.Optional[str]

class QualtricsExportStatusFailed(ImmutableBaseModel):
    status: t.Literal["failed"]

QualtricsExportStatus = t.Annotated[
    t.Union[
        QualtricsExportStatusComplete,
        QualtricsExportStatusInProgress,
        QualtricsExportStatusFailed
    ], Field(discriminator='status')
]

