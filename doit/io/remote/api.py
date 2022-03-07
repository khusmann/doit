import typing as t
from abc import ABC, abstractmethod

from pydantic import BaseModel

from ...domain.value import RemoteTableListing

from pathlib import Path

class RemoteIoApi(BaseModel, ABC):
    @abstractmethod
    def fetch_remote_table(self, remote_id: str, data_path: Path, schema_path: Path) -> None:
        pass

    @abstractmethod
    def fetch_table_listing(self) -> t.List[RemoteTableListing]:
        pass
