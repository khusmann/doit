from __future__ import annotations
from itertools import zip_longest
import typing as t
from abc import ABC, abstractmethod
from doit.domain.value.common import *
import pytest
from dataclasses import dataclass
import traceback

T = t.TypeVar('T')
P = t.TypeVar('P')

MissingReason = t.Literal['omitted', 'redacted']
ErrorReason = t.Literal['error', 'type_error', 'mapping_error', 'length_mismatch']

class Maybe(t.Generic[T], ABC):
    @abstractmethod
    def map(self, f: t.Callable[[T], P]) -> Maybe[P]:
        pass

    @abstractmethod
    def chain(self, f: t.Callable[[T], Maybe[P]]) -> Maybe[P]:
        pass

@dataclass(frozen=True)
class Some(Maybe[T]):
    value: T
    def map(self, f: t.Callable[[T], P]) -> Maybe[P]:
        return Some(f(self.value))
    def chain(self, f: t.Callable[[T], Maybe[P]]) -> Maybe[P]:
        return f(self.value)

@dataclass(frozen=True)
class Missing(Maybe[T]):
    reason: MissingReason
    def map(self, f: t.Callable[[t.Any], P]) -> Maybe[P]:
        return Missing[P](self.reason)
    def chain(self, f: t.Callable[[t.Any], Maybe[P]]) -> Maybe[P]:
        return Missing[P](self.reason)

class Error(Maybe[T]):
    reason: ErrorReason
    stack: traceback.StackSummary
    def __init__(self, reason: ErrorReason):
        self.reason = reason
        self.stack = traceback.extract_stack()
    def map(self, f: t.Callable[[t.Any], P]) -> Maybe[P]:
        return Error[P](self.reason)
    def chain(self, f: t.Callable[[t.Any], Maybe[P]]) -> Maybe[P]:
        return Error[P](self.reason)
    def __repr__(self):
        return "Error(reason={})".format(self.reason)

UnsafeStr = t.NewType('UnsafeStr', str)
ColumnValueType = str | bool | UnsafeStr | int
ColumnValueT = t.TypeVar('ColumnValueT', bound=ColumnValueType)
ColumnValueP = t.TypeVar('ColumnValueP', bound=ColumnValueType)

@dataclass(frozen=True)
class TextColumnData:
    name: ColumnName
    values: t.Tuple[Maybe[str], ...] = ()
    value_type: t.ClassVar[t.Type[str]] = str
    some_type: t.ClassVar[t.Type[Some[str]]] = Some[str]
    missing_type: t.ClassVar[t.Type[Missing[str]]] = Missing[str]
    error_type: t.ClassVar[t.Type[Error[str]]] = Error[str]

@dataclass(frozen=True)
class BoolColumnData:
    name: ColumnName
    values: t.Tuple[Maybe[bool], ...] = ()
    value_type: t.ClassVar[t.Type[bool]] = bool
    some_type: t.ClassVar[t.Type[Some[bool]]] = Some[bool]
    missing_type: t.ClassVar[t.Type[Missing[bool]]] = Missing[bool]
    error_type: t.ClassVar[t.Type[Error[bool]]] = Error[bool]

@dataclass(frozen=True)
class IntColumnData:
    name: ColumnName
    values: t.Tuple[Maybe[int], ...] = ()
    value_type: t.ClassVar[t.Type[int]] = int
    some_type: t.ClassVar[t.Type[Some[int]]] = Some[int]
    missing_type: t.ClassVar[t.Type[Missing[int]]] = Missing[int]
    error_type: t.ClassVar[t.Type[Error[int]]] = Error[int]

@dataclass(frozen=True)
class UnsafeTextColumnData:
    name: ColumnName
    values: t.Tuple[Maybe[UnsafeStr], ...] = ()
    value_type: t.ClassVar[t.Type[UnsafeStr]] = UnsafeStr
    some_type: t.ClassVar[t.Type[Some[UnsafeStr]]] = Some[UnsafeStr]
    missing_type: t.ClassVar[t.Type[Missing[UnsafeStr]]] = Missing[UnsafeStr]
    error_type: t.ClassVar[t.Type[Error[UnsafeStr]]] = Error[UnsafeStr]

ColumnType = TextColumnData | BoolColumnData | UnsafeTextColumnData | IntColumnData

columndata_type_lookup: t.Mapping[t.Type[ColumnValueType], t.Type[ColumnType]] = {
    str: TextColumnData,
    bool: BoolColumnData,
    UnsafeStr: UnsafeTextColumnData,
    int: IntColumnData,
}

def ColumnData(name: ColumnName, values: t.Iterable[Maybe[ColumnValueT]], value_type: t.Type[ColumnValueT]):
    typed_values = (Error[ColumnValueT]('type_error') if isinstance(v, Some) and type(v.value) != value_type else v for v in values)
    return columndata_type_lookup[value_type](name, tuple(typed_values)) # type: ignore

@dataclass(frozen=True)
class TableData:
    columns: t.Tuple[ColumnType, ...]

    def map_rows(
        self,
        fn: t.Callable[[t.Dict[ColumnName, Maybe[t.Any]]], t.Dict[ColumnName, Maybe[t.Any]]],
        result: TableData,
    ) -> TableData:
        new_rows_dicts = tuple(fn(rd) for rd in self.iter_rows())

        return result.map_columns(
            lambda c: ColumnData(
                name=c.name,
                values=tuple(rd.get(c.name, c.error_type('mapping_error')) for rd in new_rows_dicts),
                value_type=c.value_type
            )
        )

    def map_columns(self, fn: t.Callable[[ColumnType], ColumnType]) -> TableData:
        return TableData(
            columns=tuple((fn(c) for c in self.columns)),
        )

    def chain_columns(self, fn: t.Callable[[ColumnType], TableData]) -> TableData:
        return TableData(
            columns=tuple(
                p 
                    for c in self.columns
                        for p in fn(c).columns
            )
        )

    def iter_rows(self):
        return ({c.name: v for c, v in zip(self.columns, row)} for row in zip_longest(*(c.values for c in self.columns), fillvalue=Error('length_mismatch')))

    def __repr__(self):
        result = " | ".join((c.name for c in self.columns)) + "\n"
        for row in self.iter_rows():
            result += " | ".join(repr(v) for v in row.values()) + "\n"
        return result



#@dataclass(frozen=True)
#class SanitizerColumn(Column[str, None]): pass

#@dataclass(frozen=True)
#class Sanitizer(Table[SanitizerColumn, SanitizerMeta]): pass


#Sanitizer
#- checksum
#- last_modified
#- columns: t.Mapping[ColumnName, t.List[str]]

#TableSubset
#- columns: t.Mapping[ColumnName, t.List[str]]
#- rowwise()

#--------------------------

#SafeTable
#- name
#- source_data_checksum
#- schema_checksum
#- last_fetched_utc
#- columns: t.Mapping[SourceColumnName, SafeColumn]



# Arrange
@pytest.fixture
def column1():
    return ColumnData(
        name=ColumnName("Column1"),
        values=(Some("Val1"), Some("Val2"), Some("Val3"), Missing('omitted')),
        value_type=str,
    )

@pytest.fixture
def column2():
    return ColumnData(
        name=ColumnName("Column2"),
        values=(Some("Val1"), Some("Val2"), Some("Val3"), Missing('omitted')),
        value_type=str,
    )

@pytest.fixture
def column3():
    return ColumnData(
        name=ColumnName("Column3"),
        values=(Some(True), Missing('omitted'), Some(False), Error('error')),
        value_type=bool,
    )

@pytest.fixture
def table(column1: ColumnType, column2: ColumnType, column3: ColumnType):
    return TableData(
        columns=(column1, column2, column3)
    )

def test_filter(table: TableData):
    result = table.chain_columns(lambda c: TableData(columns=(c,)) if c.value_type != str else TableData(columns=()))
    assert [c.name for c in result.columns] == ['Column3']

def test_table(table: TableData):
    result = table.chain_columns(lambda c: TableData(columns=(c,)) if c.name != "Column2" else TableData(columns=()))
    assert [c.name for c in result.columns] == ['Column1', 'Column3']

def hash_row(row: t.Dict[ColumnName, Maybe[t.Any]], hash_cols: t.Sequence[ColumnName]) -> Maybe[int]:
    vals = tuple(row.get(name) for name in hash_cols)
    return Error('mapping_error') if any(i is None for i in vals) else Some(hash(vals))

def test_table2(table: TableData):
    result = table.map_rows(
        lambda r: { ColumnName("hash"): hash_row(r, [ColumnName("Column1"), ColumnName("Column3")]), **r },
        TableData(
            columns=(ColumnData(ColumnName("hash"), (), int), *table.columns)
        )
    )

    print(result)

    def inc(v: t.Any) -> int:
        return int(v / 1000000)

    result2 = result.map_columns(
        lambda c: ColumnData(
            ColumnName("hash1"),
            (m.map(inc) for m in c.values),
            int
        ) if c.name == "hash" and c.value_type == int else c
    )

    print(result2)

    assert "hash" in [c.name for c in result.columns]