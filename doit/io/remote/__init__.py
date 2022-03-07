import typing as t

from .api import RemoteIoApi
from .qualtrics import QualtricsRemote

from ...domain.value import RemoteTableListing, RemoteService, TableFileInfo

_impl_map: t.Mapping[RemoteService, RemoteIoApi] = {
    "qualtrics": QualtricsRemote(),
}

def fetch_remote_table(info: TableFileInfo) -> None:
    return _impl_map[info.remote_id.service].fetch_remote_table(info.remote_id.id, info.data_path, info.schema_path)

def fetch_table_listing(service: RemoteService) -> t.List[RemoteTableListing]:
    return _impl_map[service].fetch_table_listing()