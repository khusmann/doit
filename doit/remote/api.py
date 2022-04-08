import typing as t
from abc import ABC, abstractmethod

from ..domain.value import ImmutableBaseModel, RemoteTableListing, TableFetchInfo

from pathlib import Path

class RemoteIoApi(ImmutableBaseModel, ABC):
    @abstractmethod
    def fetch_remote_table(self,
        remote_id: str,
        data_path: Path,
        schema_path: Path,
        progress_callback: t.Callable[[int], None] = lambda _: None,
    ) -> TableFetchInfo:
        pass

    @abstractmethod
    def fetch_table_listing(self) -> t.List[RemoteTableListing]:
        pass
