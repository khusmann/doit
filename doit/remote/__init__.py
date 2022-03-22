import typing as t

from .api import RemoteIoApi
from .qualtrics import QualtricsRemote

from ..domain.value import RemoteTableListing, RemoteService, TableFileInfo, TableFetchInfo

_impl_map: t.Mapping[RemoteService, RemoteIoApi] = {
    "qualtrics": QualtricsRemote(),
}

def fetch_remote_table(info: TableFileInfo) -> TableFetchInfo:
    return _impl_map[info.remote.service].fetch_remote_table(info.remote.id, info.data_path, info.schema_path)

def fetch_table_listing(service: RemoteService) -> t.List[RemoteTableListing]:
    return _impl_map[service].fetch_table_listing()