from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod

from .model import (
    SanitizedTableInfo,
    SanitizedTable,
)

class SanitizedTableRepoReader(ABC):
    impls: t.ClassVar[t.Dict[str, t.Type[SanitizedTableRepoReader]]]

    @abstractmethod
    def read_tableinfo(self, name: str) -> SanitizedTableInfo: ...

    @classmethod
    @abstractmethod
    def open(cls, filename: str = "") -> SanitizedTableRepoReader: ...

class SanitizedTableRepoWriter(ABC):
    
    @abstractmethod
    def write_table(self, table: SanitizedTable, name: str) -> None: ...

    @classmethod
    @abstractmethod
    def new(cls, filename: str = "") -> SanitizedTableRepoWriter: ...