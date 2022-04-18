from __future__ import annotations
import pytest
import re
import typing as t
from itertools import zip_longest
import traceback
from dataclasses import dataclass


class UnsafeColumn(t.NamedTuple):
    unsafe_name: str

class SafeColumn(t.NamedTuple):
    name: str

T = t.TypeVar('T')

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

ValueT = t.TypeVar("ValueT")

ColumnT = t.TypeVar('ColumnT', bound=UnsafeColumn | SafeColumn)

@dataclass(frozen=True)
class TableRowView(t.Generic[ColumnT, ValueT]):
    map: t.Mapping[ColumnT, Maybe[ValueT]]
    def get(self, column: ColumnT) -> Maybe[ValueT]:
        return self.map.get(column, Error('missing_column'))
    def subset(self, keys: t.Collection[ColumnT]) -> TableRowView[ColumnT, ValueT]:
        return TableRowView({ k: self.get(k) for k in keys })
    def hash(self) -> HashValue:
        return HashValue(hash(frozenset(self.map.items())))

@dataclass(frozen=True)
class Table(t.Generic[ColumnT, ValueT]):
    values: t.Tuple[t.Tuple[Maybe[ValueT], ...], ...] # rows x columns
    columns: t.Tuple[ColumnT, ...]

    def iter_rows(self) -> t.Generator[TableRowView[ColumnT, ValueT], None, None]:
        return (
            TableRowView({
                c: v for c, v in zip(self.columns, row_values)
            }) for row_values in self.values
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns) + "\n"
        for row in self.iter_rows():
            result += " | ".join(str(row.get(c)) for c in self.columns) + "\n"
        return result

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

UnsafeTable = Table[UnsafeColumn, str]
UnsafeTableRowView = TableRowView[UnsafeColumn, str]

SafeTable = Table[SafeColumn, str]
SafeTableRowView = TableRowView[SafeColumn, str]

class Sanitizer(t.NamedTuple):
    key_col_names: t.Tuple[UnsafeColumn]
    new_col_names: t.Tuple[SafeColumn, ...]
    map: t.Mapping[HashValue, SafeTableRowView]
    def get(self, h: HashValue) -> SafeTableRowView:
        return self.map.get(h, TableRowView(
            { k: Missing('redacted') for k in self.new_col_names }
        ))

def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsafeTable):
    subset_rows = (row.subset(sanitizer.key_col_names) for row in table.iter_rows())
    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]

def sanitizer_from_raw(table: SanitizerSpec) -> Sanitizer:
    blessed_col_names = tuple(UnsafeColumn(c[1:-1]) if re.match(r'^\(.+\)$', c) else SafeColumn(c) for c in table.header)

    def handle_missing(v: str, c: UnsafeColumn | SafeColumn):
        match c:
            case UnsafeColumn():
                return Some(v) if v else Missing('omitted')
            case SafeColumn():
                return Some(v) if v else Missing('redacted')

    blessed_table = Table(
        columns=blessed_col_names,
        values=tuple(tuple(handle_missing(v, c) for v, c in zip_longest(row, blessed_col_names)) for row in table.rows)
    )

    key_col_names = tuple(c for c in blessed_col_names if isinstance(c, UnsafeColumn))
    new_col_names = tuple(c for c in blessed_col_names if isinstance(c, SafeColumn))

    key_table = Table(
        columns=key_col_names,
        values=tuple(tuple(row.subset(key_col_names).map.values()) for row in blessed_table.iter_rows())
    )

    new_table = Table(
        columns=new_col_names,
        values=tuple(tuple(row.subset(new_col_names).map.values()) for row in blessed_table.iter_rows())
    )

    hash_map = {
        key_table_row.hash(): new_table_row for key_table_row, new_table_row in zip(key_table.iter_rows(), new_table.iter_rows())
    }

    return Sanitizer(
        key_col_names=key_col_names,
        new_col_names=new_col_names,
        map=hash_map,
    )

def sanitize_row(row: UnsafeTableRowView, sanitizer: Sanitizer) -> SafeTableRowView:
    return sanitizer.get(
        row.subset(sanitizer.key_col_names).hash()
    )

def sanitize_table(table: UnsafeTable, sanitizers: t.Sequence[Sanitizer]) -> SafeTable:
    new_columns = tuple(
        name
            for sanitizer in sanitizers
                for name in sanitizer.new_col_names
    )

    sanitized_rows_grouped = (
        (sanitize_row(row, sanitizer) for sanitizer in sanitizers)
            for row in table.iter_rows()
    )

    sanitized_values = tuple(
        tuple(
            v
                for row in row_grouped if row is not None
                    for v in row.map.values()
        ) for row_grouped in sanitized_rows_grouped
    )

    return Table(
        columns=new_columns,
        values=sanitized_values,
    )


@pytest.fixture
def table() -> UnsafeTable:
    col1 = tuple(Some("<{}, 1>".format(v)) for v in range(5))
    col2 = tuple(Some("<{}, 2>".format(v)) for v in range(5))
    col3 = tuple(Some("<{}, 3>".format(v)) for v in range(5))

    return Table(
        values=tuple(tuple(v) for v in zip(col1, col2, col3)),
        columns=tuple((UnsafeColumn("col{}".format(c))) for c in range(1, 4))
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

def test_rowwise(table: UnsafeTable):
    row = next(table.iter_rows())
    assert list(row.map) == ['col1', 'col2', 'col3']

def test_hash(table: UnsafeTable, san_table: SanitizerSpec, san_table2: SanitizerSpec):
    sanitizer = sanitizer_from_raw(san_table)
    sanitizer2 = sanitizer_from_raw(san_table2)

    sanitized_table = sanitize_table(table, [sanitizer, sanitizer2])

    print(sanitized_table)

    assert 5==6



# Column names in () should be known to be private. (child_name) vs child_name

# Step 1: Import Table. Table has a mix of safe + unsafe rows and columns


# !!!! A SafeColumn sanitizer is simply an identity function

# Given a table, 