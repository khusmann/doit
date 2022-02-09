import typing as t
import os

from urllib.parse import urlparse

from . import qualtrics

_qualtrics_api_impl = qualtrics.QualtricsApiImpl(
    api_key=os.environ['QUALTRICS_API_KEY'],
    data_center=os.environ['QUALTRICS_DATA_CENTER'],
)

_schema_repos: t.Mapping[str, qualtrics.QualtricsSchemaRepo] = {
    'qualtrics': qualtrics.QualtricsSchemaRepo(impl=_qualtrics_api_impl)
}

_blob_repos: t.Mapping[str, qualtrics.QualtricsBlobRepo] = {
    'qualtrics': qualtrics.QualtricsBlobRepo(impl=_qualtrics_api_impl)
}

#def load_schema(uri: str):
#    return _schema_repos[urlparse(uri).scheme].load(uri)

def list_available_schemas(service: str):
    return _schema_repos[service].list_available()

def download_blob(uri: str):
    _schema_repos[urlparse(uri).scheme].download(uri)
    _blob_repos[urlparse(uri).scheme].download(uri)