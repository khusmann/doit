import typing as t
from abc import ABC, abstractmethod

from ..domain.value import ImmutableBaseModel, RemoteTableListing, TableFetchInfo

from pathlib import Path

class Progressable(t.Protocol):
    def update(self, n: int) -> None: ...
    def close(self) -> None: ...

class RemoteIoApi(ImmutableBaseModel, ABC):
    @abstractmethod
    def fetch_remote_table(self,
        remote_id: str,
        data_path: Path,
        schema_path: Path,
        progress_callback: Progressable
    ) -> TableFetchInfo:
        pass

    @abstractmethod
    def fetch_table_listing(self) -> t.List[RemoteTableListing]:
        pass
