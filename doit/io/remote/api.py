import typing as t
from abc import ABC, abstractmethod

from ...domain.value import ImmutableBaseModel, RemoteTableListing

from pathlib import Path

class RemoteIoApi(ImmutableBaseModel, ABC):
    @abstractmethod
    def fetch_remote_table(self, remote_id: str, data_path: Path, schema_path: Path) -> None:
        pass

    @abstractmethod
    def fetch_table_listing(self) -> t.List[RemoteTableListing]:
        pass
