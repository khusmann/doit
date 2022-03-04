import typing as t

from .api import RemoteIoApi
from .qualtrics import QualtricsRemote

from ...domain.value import RemoteTableListing, RemoteInfo, RemoteService

from pathlib import Path

_impl_map: t.Mapping[RemoteService, RemoteIoApi] = {
    "qualtrics": QualtricsRemote(),
}

def fetch_remote_table(remote_info: RemoteInfo, data_path: Path, schema_path: Path) -> None:
    return _impl_map[remote_info.service].fetch_remote_table(remote_info.id, data_path, schema_path)

def fetch_table_listing(remote_service: RemoteService) -> t.List[RemoteTableListing]:
    return _impl_map[remote_service].fetch_table_listing()