import typing as t
from pydantic import BaseModel, BaseSettings, Field

class QualtricsRemoteSettings(BaseSettings):
    api_key: t.Optional[str]
    data_center: t.Optional[str]
    api_url = "https://{data_center}.qualtrics.com/API/v3/{endpoint}"

    class Config(BaseSettings.Config):
        env_prefix = "qualtrics_"

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

