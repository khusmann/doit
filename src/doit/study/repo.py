from __future__ import annotations
from abc import ABC, abstractmethod

from .spec import StudySpec

from .view import (
    InstrumentView,
    MeasureView,
    ColumnView,
)

class StudyRepoWriter(ABC):

    @abstractmethod
    def query_linker(self, instrument_name: str): ... # TODO return type Linker
    
    # In service: link_table(table: SanitizedTable, linker: Linker) -> LinkedTable
    
    @abstractmethod
    def write_table(self, table: str): ... # TODO Change to type LinkedTable

    @classmethod
    @abstractmethod
    def new(cls, spec: StudySpec, filename: str = "") -> StudyRepoWriter: ...

class StudyRepoReader(ABC):

    @abstractmethod
    def query_instrument(self, instrument_name: str) -> InstrumentView: ...

    @abstractmethod
    def query_measure(self, measure_name: str) -> MeasureView: ...

    @abstractmethod
    def query_column(self, column_name: str) -> ColumnView: ...

    # def query_table(self, columns: t.Sequence[str]) -> SubsetView: ...

    @classmethod
    @abstractmethod
    def open(cls, filename: str = "") -> StudyRepoReader: ...