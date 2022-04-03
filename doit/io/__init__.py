import typing as t

from .api import UnsafeTableIoApi
from .qualtrics import QualtricsUnsafeTableIo

from ..domain.value import TableImport, TableFileInfo

_impl_map: t.Mapping[str, UnsafeTableIoApi] = {
    "qualtrics": QualtricsUnsafeTableIo()
}

def read_unsafe_table_data(info: TableFileInfo) -> TableImport:
    return _impl_map[info.format].read_unsafe_table_data(info.data_path, info.schema_path)