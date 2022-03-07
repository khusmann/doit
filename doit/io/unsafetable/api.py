from abc import ABC, abstractmethod

from pydantic import BaseModel

from ...domain.value import TableImport
from pathlib import Path

class UnsafeTableIoApi(BaseModel, ABC):
    @abstractmethod
    def read_unsafe_table_data(self, data_path: Path, schema_path: Path) -> TableImport:
        pass