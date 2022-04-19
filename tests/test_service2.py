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
    id: UnsanitizedColumnId
    prompt: str

class SanitizedColumn(t.NamedTuple):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: str

T = t.TypeVar('T')
P = t.TypeVar("P")

MissingReason = t.Literal['omitted', 'redacted']
ErrorReason = t.Literal['error', 'type_error', 'mapping_error', 'length_mismatch', 'missing_column', 'missing_sanitizer']

@dataclass(frozen=True)
class Some(t.Generic[T]):
    value: T
    def is_type(self, some_type: t.Any) -> t.TypeGuard[Some[T]]:
        return type(self.value) == some_type 


class Missing(t.NamedTuple):
    reason: MissingReason
    def is_type(self, _: t.Any) -> t.TypeGuard[Missing]:
        return True

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
    def is_type(self, _: t.Any) -> t.TypeGuard[Error]:
        return True

TableValue = Some[T] | Missing | Error

RowViewHash = t.NewType('RowViewHash', int)

MaybeRowViewHash = Some[RowViewHash] | Error

class UnsanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedColumnId(t.NamedTuple):
    name: str

ColumnIdT = t.TypeVar('ColumnIdT', UnsanitizedColumnId, SanitizedColumnId)

@dataclass(frozen=True)
class TableRowView(t.Generic[ColumnIdT, T]):
    values: t.Mapping[ColumnIdT, TableValue[T]]
    def get(self, column_name: ColumnIdT) -> TableValue[T]:
        return self.values.get(column_name, Error('missing_column'))
    def subset(self, keys: t.Collection[ColumnIdT]) -> TableRowView[ColumnIdT, T]:
        return TableRowView({ k: self.get(k) for k in keys })
    def subset_type(self, value_type: t.Type[P]) -> TableRowView[ColumnIdT, P]:
        return TableRowView({ k: v for k, v in self.values.items() if v.is_type(value_type)})
    def hash(self) -> MaybeRowViewHash:
        error = next((v for v in self.values.values() if isinstance(v, Error)), None)
        return error if error else Some(RowViewHash(hash(frozenset((k, v) for k, v in self.values.items()))))
    def hash_or_die(self) -> RowViewHash:
        val = self.hash()
        match val:
            case Some():
                return val.value
            case Error():
                raise Exception("Unexpected error value: {}".format(val))
    
@dataclass(frozen=True)
class SanitizedTable:
    values: t.Tuple[t.Tuple[TableValue[t.Any], ...], ...] # rows x columns
    columns: t.Tuple[SanitizedColumn, ...]

    def iter_rows(self):
        return (
            TableRowView({
                c.id: v for c, v in zip(self.columns, row_values)
            }) for row_values in self.values
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns) + "\n"
        for row in self.iter_rows():
            result += " | ".join(str(row.get(c.id)) for c in self.columns) + "\n"
        return result

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

@dataclass(frozen=True)
class UnsanitizedTable:
    values: t.Tuple[t.Tuple[TableValue[t.Any], ...], ...] # rows x columns
    columns: t.Tuple[UnsanitizedColumn, ...]

    def iter_rows(self):
        return (
            TableRowView({
                c.id: v for c, v in zip(self.columns, row_values)
            }) for row_values in self.values
        )

    def iter_str_rows(self):
        return (
            row.subset_type(str) for row in self.iter_rows()
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns) + "\n"
        for row in self.iter_rows():
            result += " | ".join(str(row.get(c.id)) for c in self.columns) + "\n"
        return result

UnsanitizedStrTableRowView = TableRowView[UnsanitizedColumnId, str]
SanitizedStrTableRowView = TableRowView[SanitizedColumnId, str]

class Sanitizer(t.NamedTuple):
    key_col_ids: t.Tuple[UnsanitizedColumnId, ...]
    new_col_ids: t.Tuple[SanitizedColumnId, ...]
    map: t.Mapping[RowViewHash, SanitizedStrTableRowView]
    def get(self, h: MaybeRowViewHash) -> SanitizedStrTableRowView:
        match h:
            case Some():
                return self.map.get(h.value, TableRowView(
                    { k: Missing('redacted') for k in self.new_col_ids }
                ))
            case Error():
                return TableRowView({ k: h for k in self.new_col_ids })

def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsanitizedTable):
    subset_rows = (row.subset(sanitizer.key_col_ids) for row in table.iter_rows())
    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]

def sanitizer_from_raw(table: SanitizerSpec) -> Sanitizer:
    key_col_names = {c: UnsanitizedColumnId(c[1:-1]) for c in table.header if re.match(r'^\(.+\)$', c)}
    new_col_names = {c: SanitizedColumnId(c) for c in table.header if c not in key_col_names}

    key_col_rows = (
        UnsanitizedStrTableRowView({
            key_col_names[c]: Some(v) if v else Missing('omitted')
                for c, v in zip_longest(table.header, row) if c in key_col_names
        }) for row in table.rows
    )

    new_col_rows = (
        SanitizedStrTableRowView({
            new_col_names[c]: Some(v) if v else Missing('redacted')
                for c, v in zip_longest(table.header, row) if c in new_col_names
        }) for row in table.rows
    )

    hash_map = {
        key.hash_or_die(): new for key, new in zip(key_col_rows, new_col_rows)
    }

    return Sanitizer(
        key_col_ids=tuple(key_col_names.values()),
        new_col_ids=tuple(new_col_names.values()),
        map=hash_map,
    )

def sanitize_row(row: UnsanitizedStrTableRowView, sanitizers: t.Sequence[Sanitizer]) -> SanitizedStrTableRowView:
    return SanitizedStrTableRowView(dict(
        keypair
            for sanitizer in sanitizers
                for keypair in sanitizer.get(row.subset(sanitizer.key_col_ids).hash()).values.items()
    ))

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[Sanitizer]) -> SanitizedTable:
    sanitized_columns = tuple(
        SanitizedColumn(
            name,
            prompt="; ".join(c.prompt for c in table.columns if c.id in sanitizer.key_col_ids),
            sanitizer_checksum="boop"
        )   for sanitizer in sanitizers
                for name in sanitizer.new_col_ids
    )

    sanitized_row_view = (
        sanitize_row(row, sanitizers) for row in table.iter_str_rows()
    )

    sanitized_values = tuple(
        tuple(row.get(c.id) for c in sanitized_columns) for row in sanitized_row_view
    )

    return SanitizedTable(
        columns=sanitized_columns,
        values=sanitized_values,
        # Table Info goes here...
    )


@pytest.fixture
def table() -> UnsanitizedTable:
    col1 = tuple(Some("<{}, 1>".format(v)) for v in range(5))
    col2 = tuple(Some("<{}, 2>".format(v)) for v in range(5))
    col3 = tuple(Some("<{}, 3>".format(v)) for v in range(5))

    return UnsanitizedTable(
        values=tuple(tuple(v) for v in zip(col1, col2, col3)),
        columns=tuple((UnsanitizedColumn(UnsanitizedColumnId("col{}".format(c)), prompt="asdf")) for c in range(1, 4))
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