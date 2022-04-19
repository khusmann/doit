from __future__ import annotations
import pytest
import re
import typing as t
from itertools import zip_longest
import traceback
from dataclasses import dataclass
from pydantic import BaseModel

class TableImportInfo(BaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[ColumnImportInfo, ...]

class TextColumnImportInfo(BaseModel):
    type: t.Literal['text']
    prompt: str
    needs_sanitization: bool

class OrdinalColumnImportInfo(BaseModel):
    type: t.Literal['ordinal']
    prompt: str
    codes: t.Tuple[str, ...]

class BoolColumnImportInfo(BaseModel):
    type: t.Literal['bool']
    prompt: str

@dataclass(frozen=True)
class TableImport:
    info: TableImportInfo
    rows: t.Tuple[t.Tuple[t.Optional[str | bool], ...], ...]

ColumnImportInfo = TextColumnImportInfo | OrdinalColumnImportInfo | BoolColumnImportInfo

######################

class SourceTableInfo(BaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SourceColumnInfo, ...]

class TextSourceColumnInfo(BaseModel):
    type: t.Literal['text']
    prompt: str
    sanitizer_checksum: t.Optional[str]

class OrdinalSourceColumnInfo(BaseModel):
    type: t.Literal['ordinal']
    prompt: str


SourceColumnInfo = TextSourceColumnInfo

#@dataclass(frozen=True)
class SourceTable:
    info: SourceTableInfo


def sanitize_table_import(table_import: TableImport, sanitizer_specs: t.Sequence[SanitizerSpec]) -> SourceTable:
    return SourceTable()

######################

class UnsanitizedColumn(t.NamedTuple):
    name: UnsanitizedColumnName
    prompt: str

class SanitizedColumn(t.NamedTuple):
    name: SanitizedColumnName
    prompt: str
    sanitizer_checksum: str

T = t.TypeVar('T', covariant=True)

MissingReason = t.Literal['omitted', 'redacted']
ErrorReason = t.Literal['error', 'type_error', 'mapping_error', 'length_mismatch', 'missing_column', 'missing_sanitizer']

@dataclass(frozen=True)
class Some(t.Generic[T]):
    value: T

class Missing(t.NamedTuple):
    reason: MissingReason

class Error:
    reason: ErrorReason
    stack: traceback.StackSummary
    def __init__(self, reason: ErrorReason):
        self.reason = reason
        self.stack = traceback.extract_stack()
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return "Error(reason={})".format(self.reason)

Maybe = Some[T] | Missing | Error

HashValue = t.NewType('HashValue', int)

class UnsanitizedColumnName(str): pass
class SanitizedColumnName(str): pass

ValueT = t.TypeVar("ValueT")
ColumnNameT = t.TypeVar('ColumnNameT', UnsanitizedColumnName, SanitizedColumnName)

@dataclass(frozen=True)
class TableRowView(t.Generic[ColumnNameT, ValueT]):
    values: t.Mapping[ColumnNameT, Maybe[ValueT]]
    def get(self, column_name: ColumnNameT) -> Maybe[ValueT]:
        return self.values.get(column_name, Error('missing_column'))
    def subset(self, keys: t.Collection[ColumnNameT]) -> TableRowView[ColumnNameT, ValueT]:
        return TableRowView({ k: self.get(k) for k in keys })
    def hash(self) -> HashValue:
        return HashValue(hash(frozenset((k, v) for k, v in self.values.items())))

@dataclass(frozen=True)
class SanitizedTable:
    values: t.Tuple[t.Tuple[Maybe[str], ...], ...] # rows x columns
    columns: t.Tuple[SanitizedColumn, ...]

    def iter_rows(self):
        return (
            TableRowView({
                c.name: v for c, v in zip(self.columns, row_values)
            }) for row_values in self.values
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns) + "\n"
        for row in self.iter_rows():
            result += " | ".join(str(row.get(c.name)) for c in self.columns) + "\n"
        return result

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

@dataclass(frozen=True)
class UnsanitizedTable:
    values: t.Tuple[t.Tuple[Maybe[str | bool], ...], ...] # rows x columns
    columns: t.Tuple[UnsanitizedColumn, ...]

    def iter_rows(self):
        return (
            TableRowView({
                c.name: v for c, v in zip(self.columns, row_values)
            }) for row_values in self.values
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns) + "\n"
        for row in self.iter_rows():
            result += " | ".join(str(row.get(c.name)) for c in self.columns) + "\n"
        return result

UnsanitizedTableRowView = TableRowView[UnsanitizedColumnName, str]
SanitizedTableRowView = TableRowView[SanitizedColumnName, str]

class Sanitizer(t.NamedTuple):
    key_col_names: t.Tuple[UnsanitizedColumnName, ...]
    new_col_names: t.Tuple[SanitizedColumnName, ...]
    map: t.Mapping[HashValue, SanitizedTableRowView]
    def get(self, h: HashValue) -> SanitizedTableRowView:
        return self.map.get(h, TableRowView(
            { k: Missing('redacted') for k in self.new_col_names }
        ))

def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsanitizedTable):
    subset_rows = (row.subset(sanitizer.key_col_names) for row in table.iter_rows())
    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]

def sanitizer_from_raw(table: SanitizerSpec) -> Sanitizer:
    key_col_names = {c: UnsanitizedColumnName(c[1:-1]) for c in table.header if re.match(r'^\(.+\)$', c)}
    new_col_names = {c: SanitizedColumnName(c) for c in table.header if c not in key_col_names}

    key_col_rows = (
        UnsanitizedTableRowView({
            key_col_names[c]: Some(v) if v else Missing('omitted')
                for c, v in zip_longest(table.header, row) if c in key_col_names
        }) for row in table.rows
    )

    new_col_rows = (
        SanitizedTableRowView({
            new_col_names[c]: Some(v) if v else Missing('redacted')
                for c, v in zip_longest(table.header, row) if c in new_col_names
        }) for row in table.rows
    )

    hash_map = {
        key.hash(): new for key, new in zip(key_col_rows, new_col_rows)
    }

    return Sanitizer(
        key_col_names=tuple(key_col_names.values()),
        new_col_names=tuple(new_col_names.values()),
        map=hash_map,
    )

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[Sanitizer]) -> SanitizedTable:
    sanitized_columns = tuple(
        SanitizedColumn(
            name,
            prompt="; ".join(c.name for c in table.columns if c.name in sanitizer.key_col_names),
            sanitizer_checksum="boop"
        )   for sanitizer in sanitizers
                for name in sanitizer.new_col_names
    )

    sanitized_rows_grouped = (
        (
            sanitizer.get(
                row.subset(sanitizer.key_col_names).hash()
            ) for sanitizer in sanitizers
        ) for row in table.iter_rows()
    )

    sanitized_values = tuple(
        tuple(
            v
                for row in row_grouped
                    for v in row.values.values()
        ) for row_grouped in sanitized_rows_grouped
    )

    return SanitizedTable(
        columns=sanitized_columns,
        values=sanitized_values,
    )


@pytest.fixture
def table() -> UnsanitizedTable:
    col1 = tuple(Some("<{}, 1>".format(v)) for v in range(5))
    col2 = tuple(Some("<{}, 2>".format(v)) for v in range(5))
    col3 = tuple(Some("<{}, 3>".format(v)) for v in range(5))

    return UnsanitizedTable(
        values=tuple(tuple(v) for v in zip(col1, col2, col3)),
        columns=tuple((UnsanitizedColumn(UnsanitizedColumnName("col{}".format(c)), prompt="asdf")) for c in range(1, 4))
    ) 

@pytest.fixture
def san_table() -> SanitizerSpec:
    col1 = tuple("<{}, 1>".format(v) for v in range(5))
    col2 = (*tuple("<{}, sanitized>".format(v) for v in range(4)), "")

    colinfo1 = "(col1)"
    colinfo2 = "sanitized_col1"

    return SanitizerSpec(
        header=(colinfo1, colinfo2),
        rows=tuple(tuple(v) for v in zip(col1, col2)),
    ) 

@pytest.fixture
def san_table2() -> SanitizerSpec:
    col1 = tuple("<{}, 3>".format(v) for v in range(5))
    col2 = tuple("<{}, sanitized3>".format(v) for v in range(5))

    colinfo1 = "(col3)"
    colinfo2 = "sanitized_col3"

    return SanitizerSpec(
        header=(colinfo1, colinfo2),
        rows=tuple(tuple(v) for v in zip(col1, col2)),
    ) 

def test_rowwise(table: UnsanitizedTable):
    row = next(table.iter_rows())
    assert list(row.values) == ['col1', 'col2', 'col3']

def test_hash(table: UnsanitizedTable, san_table: SanitizerSpec, san_table2: SanitizerSpec):
    sanitizer = sanitizer_from_raw(san_table)
    sanitizer2 = sanitizer_from_raw(san_table2)

    sanitized_table = sanitize_table(table, [sanitizer, sanitizer2])

    print(sanitized_table)

    assert 5==6



# Column names in () should be known to be private. (child_name) vs child_name

# Step 1: Import Table. Table has a mix of safe + unsafe rows and columns


# !!!! A SafeColumn sanitizer is simply an identity function

# Given a table, 