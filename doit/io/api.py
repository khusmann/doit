from abc import ABC, abstractmethod
import typing as t
from ..domain.value import ImmutableBaseModel, ColumnImport
from pathlib import Path

class UnsafeTableIoApi(ImmutableBaseModel, ABC):
    @abstractmethod
    def read_unsafe_table_data(self, data_path: Path, schema_path: Path) -> t.Tuple[ColumnImport, ...]:
        pass