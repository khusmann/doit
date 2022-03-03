import typing as t

from .api import UnsafeTableIoApi
from .qualtrics import QualtricsUnsafeTableIo

from ...domain.value import UnsafeTable 

from pathlib import Path

_impl_map: t.Mapping[str, UnsafeTableIoApi] = {
    "qualtrics": QualtricsUnsafeTableIo()
}

def read_unsafe_table(format: str, data_path: Path, schema_path: Path) -> UnsafeTable:
    return _impl_map[format].read_unsafe_table(data_path, schema_path)