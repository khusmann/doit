import typing as t
from pydantic import BaseModel
from abc import ABC, abstractmethod
from pathlib import Path

class RemoteInstrumentDesc(BaseModel):
    uri: str
    title: str

class UnsafeDataColumn(BaseModel):
    column_id: str
    prompt: str
    type: t.Literal['category', 'string', 'numeric', 'bool']
    data: t.Union[t.List[t.Optional[str]], t.List[t.Optional[bool]]]
    # TODO: Add verification that bool -> list[bool]

class UnsafeTable(BaseModel):
    title: str
    columns: t.Mapping[str, UnsafeDataColumn]

class InstrumentSourceImpl(ABC):
    @abstractmethod
    def fetch(self, workdir: Path, uri: str) -> None:
        pass

    @abstractmethod
    def load_data(self, workdir: Path, uri: str) -> UnsafeTable:
        pass

    @abstractmethod
    def fetch_available_desc(self) -> t.List[RemoteInstrumentDesc]:
        pass

class TestSourceImpl(InstrumentSourceImpl):
    def fetch(self, workdir: Path, uri: str) -> None:
        print("Fetching... {} into {}".format(uri, workdir))

    def load_data(self, workdir: Path, uri: str) -> UnsafeTable:
        return UnsafeTable(
            title = "Test Data",
            data = {}
        )

    def fetch_available_desc(self) -> t.List[RemoteInstrumentDesc]:
        return [
            RemoteInstrumentDesc(
                uri = "test://blah",
                title = "A test item"
            ),
            RemoteInstrumentDesc(
                uri = "test://blah2",
                title = "Another test item"
            ),
        ]

