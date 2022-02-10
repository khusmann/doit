import typing as t
import os

from urllib.parse import urlparse

from .qualtrics import QualtricsApiImpl, QualtricsSchemaRepo, QualtricsBlobRepo
from .local import InstrumentConfigRepoImpl

from ..domain import instrumentconfig

_qualtrics_api_impl = QualtricsApiImpl(
    api_key=os.environ['QUALTRICS_API_KEY'],
    data_center=os.environ['QUALTRICS_DATA_CENTER'],
)

_instrument_repo = InstrumentConfigRepoImpl()

_schema_repos: t.Mapping[str, QualtricsSchemaRepo] = {
    'qualtrics': QualtricsSchemaRepo(impl=_qualtrics_api_impl)
}

_blob_repos: t.Mapping[str, QualtricsBlobRepo] = {
    'qualtrics': QualtricsBlobRepo(impl=_qualtrics_api_impl)
}

def save_instrument_config(instrument: instrumentconfig.InstrumentConfig):
    _instrument_repo.save(instrument)

def load_schema(uri: str):
    return _schema_repos[urlparse(uri).scheme].load(uri)

def list_available_schemas(service: str):
    return _schema_repos[service].list_available()

def download_blob(uri: str):
    _schema_repos[urlparse(uri).scheme].download(uri)
    _blob_repos[urlparse(uri).scheme].download(uri)