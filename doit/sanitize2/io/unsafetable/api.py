from abc import ABC, abstractmethod

from pydantic import BaseModel

from ...domain.value import UnsafeTable
from pathlib import Path

class UnsafeTableIoApi(BaseModel, ABC):
    @abstractmethod
    def read_unsafe_table(self, data_path: Path, schema_path: Path) -> UnsafeTable:
        pass