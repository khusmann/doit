import typing as t

from .api import RemoteIoApi
from .qualtrics import QualtricsRemote

from ...domain.value import RemoteTableInfo

from pathlib import Path

_impl_map: t.Mapping[str, RemoteIoApi] = {
    "qualtrics": QualtricsRemote()
}

def fetch_remote_table(service: str, remote_id: str, data_path: Path, schema_path: Path) -> None:
    _impl_map[service].fetch_remote_table(remote_id, data_path, schema_path)

def fetch_table_listing(service: str) -> t.List[RemoteTableInfo]:
    return _impl_map[service].fetch_table_listing()