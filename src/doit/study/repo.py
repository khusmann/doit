from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod

from ..common.table import TableErrorReport

from .spec import StudySpec

from .model import LinkedTable

from .view import (
    InstrumentView,
    MeasureView,
    ColumnView,
    InstrumentLinkerSpec,
    ColumnRawView,
)

class StudyRepoWriter(ABC):
    @abstractmethod
    def query_instrumentlinkerspecs(self) -> t.Tuple[InstrumentLinkerSpec, ...]: ...

    @abstractmethod
    def write_table(self, linked_table: LinkedTable) -> TableErrorReport: ...

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

    @abstractmethod
    def query_column_raw(self, patterns: t.Sequence[str]) -> t.Tuple[ColumnRawView, ...]: ...

    # def query_table(self, columns: t.Sequence[str]) -> SubsetView: ...

    @classmethod
    @abstractmethod
    def open(cls, filename: str = "") -> StudyRepoReader: ...