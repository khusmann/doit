from __future__ import annotations
import pytest
import re
import typing as t
from itertools import zip_longest
import traceback
from dataclasses import dataclass
from pydantic import BaseModel

### Common types

class UnsanitizedColumnId(t.NamedTuple):
    name: str

class SanitizedColumnId(t.NamedTuple):
    name: str

T = t.TypeVar('T')
P = t.TypeVar("P")

MissingReason = t.Literal['omitted', 'redacted']
ErrorReason = t.Literal['unknown_error', 'missing_column']

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
    info: t.Any
    stack: traceback.StackSummary
    def __init__(self, reason: ErrorReason, info: t.Any = None):
        self.reason = reason
        self.info = info
        self.stack = traceback.extract_stack()
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return "Error(reason={},info={})".format(self.reason, self.info)
    def is_type(self, _: t.Any) -> t.TypeGuard[Error]:
        return True

RowViewHash = t.NewType('RowViewHash', int)
MaybeRowViewHash = Some[RowViewHash] | Error

ColumnIdT = t.TypeVar('ColumnIdT', UnsanitizedColumnId, SanitizedColumnId)
TableValue = Some[T] | Missing | Error

@dataclass(frozen=True)
class TableRowView(t.Generic[ColumnIdT, T]):
    map: t.Mapping[ColumnIdT, TableValue[T]]

    def get(self, column_name: ColumnIdT) -> TableValue[T]:
        return self.map.get(column_name, Error('missing_column', column_name))

    def subset(self, keys: t.Collection[ColumnIdT]) -> TableRowView[ColumnIdT, T]:
        return TableRowView({ k: self.get(k) for k in keys })

    def subset_type(self, value_type: t.Type[P]) -> TableRowView[ColumnIdT, P]:
        return TableRowView({ k: v for k, v in self.map.items() if v.is_type(value_type)})

    def hash(self) -> MaybeRowViewHash:
        error = next((v for v in self.map.values() if isinstance(v, Error)), None)
        return error if error else Some(RowViewHash(hash(frozenset((k, v) for k, v in self.map.items()))))

    def hash_or_die(self) -> RowViewHash:
        val = self.hash()
        match val:
            case Some():
                return val.value
            case Error():
                raise Exception("Unexpected error value: {}".format(val))

#### UnsanitizedTable

class UnsanitizedColumn(BaseModel):
    id: UnsanitizedColumnId
    prompt: str
    type: t.Literal['text', 'bool', 'ordinal']
    is_safe: bool

class UnsanitizedTableSpec(BaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[UnsanitizedColumn, ...]

@dataclass(frozen=True)
class UnsanitizedTable:
    spec: UnsanitizedTableSpec
    values: t.Tuple[t.Tuple[TableValue[t.Any], ...], ...] # rows x columns

    @property
    def columns(self):
        return self.spec.columns

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

    # Constraint: Column Meta == columns
    # Constraint: All columns are same size

#### SanitizedTable

class SanitizedColumn(BaseModel):
    id: SanitizedColumnId
    prompt: str
    sanitizer_checksum: str

class SanitizedTableSpec(BaseModel):
    data_checksum: str
    schema_checksum: str
    columns: t.Tuple[SanitizedColumn, ...]

@dataclass(frozen=True)
class SanitizedTable:
    spec: SanitizedTableSpec
    values: t.Tuple[t.Tuple[TableValue[t.Any], ...], ...] # rows x columns

    @property
    def columns(self):
        return self.spec.columns

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

#### Sanitizer

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

class SanitizerSpec(t.NamedTuple):
    header: t.Tuple[str, ...]
    rows: t.Tuple[t.Tuple[str, ...], ...]

### Service

def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsanitizedTable):
    subset_rows = (row.subset(sanitizer.key_col_ids) for row in table.iter_rows())
    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

def sanitizer_from_spec(sanitizer_spec: SanitizerSpec) -> Sanitizer:
    key_col_names = {c: UnsanitizedColumnId(c[1:-1]) for c in sanitizer_spec.header if re.match(r'^\(.+\)$', c)}
    new_col_names = {c: SanitizedColumnId(c) for c in sanitizer_spec.header if c not in key_col_names}

    keys = (
        UnsanitizedStrTableRowView({
            key_col_names[c]: Some(v) if v else Missing('omitted')
                for c, v in zip_longest(sanitizer_spec.header, row) if c in key_col_names
        }) for row in sanitizer_spec.rows
    )

    values = (
        SanitizedStrTableRowView({
            new_col_names[c]: Some(v) if v else Missing('redacted')
                for c, v in zip_longest(sanitizer_spec.header, row)if c in new_col_names
        }) for row in sanitizer_spec.rows
    )

    hash_map = {
        key.hash_or_die(): new
            for key, new in zip(keys, values)
                if any(v for v in key.map.values()) # TODO: test sanitizers with blank key columns
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
                for keypair in sanitizer.get(row.subset(sanitizer.key_col_ids).hash()).map.items()
    ))

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[Sanitizer]) -> SanitizedTable:
    sanitized_columns = tuple(
        SanitizedColumn(
            id=id,
            prompt="; ".join(c.prompt for c in table.columns if c.id in sanitizer.key_col_ids),
            sanitizer_checksum="boop"
        )   for sanitizer in sanitizers
                for id in sanitizer.new_col_ids
    )

    sanitized_spec = SanitizedTableSpec(
        data_checksum=table.spec.data_checksum,
        schema_checksum=table.spec.schema_checksum,
        columns=sanitized_columns
    )

    sanitized_row_view = (
        sanitize_row(row, sanitizers) for row in table.iter_str_rows()
    )

    sanitized_values = tuple(
        tuple(row.get(c.id) for c in sanitized_columns) for row in sanitized_row_view
    )

    return SanitizedTable(
        spec=sanitized_spec,
        values=sanitized_values,
        # Table Info goes here...
    )

### Tests

@pytest.fixture
def table() -> UnsanitizedTable:
    col1 = tuple(Some("<{}, 1>".format(v)) for v in range(5))
    col2 = tuple(Some("<{}, 2>".format(v)) for v in range(5))
    col3 = tuple(Some("<{}, 3>".format(v)) for v in range(5))

    spec = UnsanitizedTableSpec(
        data_checksum="data_checksum",
        schema_checksum="schema_checksum",
        columns=tuple(
            UnsanitizedColumn(
                id=UnsanitizedColumnId("col{}".format(c)),
                prompt="prompt {}".format(c),
                type="text",
                is_safe=True,
            ) for c in range(1, 4)
        )
    )

    return UnsanitizedTable(
        spec=spec,
        values=tuple(tuple(v) for v in zip(col1, col2, col3)),
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
    assert list(row.map) == ['col1', 'col2', 'col3']

def test_hash(table: UnsanitizedTable, san_table: SanitizerSpec, san_table2: SanitizerSpec):
    sanitizer = sanitizer_from_spec(san_table)
    sanitizer2 = sanitizer_from_spec(san_table2)

    sanitized_table = sanitize_table(table, [sanitizer, sanitizer2])

    print(sanitized_table)

    assert 5==6



# Column names in () should be known to be private. (child_name) vs child_name

# Step 1: Import Table. Table has a mix of safe + unsafe rows and columns


# !!!! A SafeColumn sanitizer is simply an identity function

# Given a table, 