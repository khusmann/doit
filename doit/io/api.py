from abc import ABC, abstractmethod

from ..domain.value import ImmutableBaseModel, TableImport
from pathlib import Path

class UnsafeTableIoApi(ImmutableBaseModel, ABC):
    @abstractmethod
    def read_unsafe_table_data(self, data_path: Path, schema_path: Path) -> TableImport:
        pass