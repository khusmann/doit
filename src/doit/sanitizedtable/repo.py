from __future__ import annotations
from abc import ABC, abstractmethod

from ..common.table import TableErrorReport

from .model import (
    SanitizedTableInfo,
    SanitizedTable,
)

class SanitizedTableRepoReader(ABC):
    @abstractmethod
    def read_tableinfo(self, name: str) -> SanitizedTableInfo: ...

    @abstractmethod
    def read_table(self, name: str) -> SanitizedTable: ...

    @classmethod
    @abstractmethod
    def open(cls, filename: str = "") -> SanitizedTableRepoReader: ...

class SanitizedTableRepoWriter(ABC):
    @abstractmethod
    def write_table(self, table: SanitizedTable) -> TableErrorReport: ...

    @classmethod
    @abstractmethod
    def new(cls, filename: str = "") -> SanitizedTableRepoWriter: ...