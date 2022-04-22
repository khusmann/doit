from __future__ import annotations
import typing as t
import traceback
from dataclasses import dataclass

### Value types

OrdinalValue = t.NewType('OrdinalValue', int)
OrdinalLabel = t.NewType('OrdinalValue', str)

### Error types

class EmptyHeaderError(ValueError):
    pass

class DuplicateHeaderError(ValueError):
    pass

class EmptySanitizerKeyError(ValueError):
    pass

# Typevars

T = t.TypeVar('T')
P = t.TypeVar("P")

ColumnIdT = t.TypeVar('ColumnIdT')
ColumnIdP = t.TypeVar('ColumnIdP')

### Maybe[T]

@dataclass(frozen=True)
class Some(t.Generic[T]):
    value: T
    def is_type(self, some_type: t.Any) -> t.TypeGuard[Some[T]]:
        return type(self.value) == some_type

class Omitted(t.NamedTuple):
    def is_type(self, _: t.Any) -> t.TypeGuard[Omitted]:
        return True

class Redacted(t.NamedTuple):
    def is_type(self, _: t.Any) -> t.TypeGuard[Omitted]:
        return True

class ColumnNotFoundInRow(t.NamedTuple):
    missing_column: t.Any
    row: t.Any

class LookupSanitizerMiss(t.NamedTuple):
    lookup: t.Any
    sanitizer_map: t.Any

ErrorReason = ColumnNotFoundInRow | LookupSanitizerMiss

class ErrorValue:
    stack: traceback.StackSummary
    reason: ErrorReason

    def __init__(self, reason: ErrorReason):
        self.stack = traceback.extract_stack()
        self.reason = reason

    def __repr__(self):
        return "{}".format(str(self.reason))

    def is_type(self, _: t.Any) -> t.TypeGuard[ErrorValue]:
        return True

    def __eq__(self, o: t.Any):
        return isinstance(o, ErrorValue) and self.reason == o.reason

    def print_traceback(self):
        print("".join(traceback.format_list(self.stack)))

TableValue = Some[T] | Omitted | Redacted | ErrorValue

def omitted_if_empty(value: t.Optional[T]) -> TableValue[T]:
    return Some(value) if value else Omitted()

def redacted_if_empty(value: t.Optional[T]) -> TableValue[T]:
    return Some(value) if value else Redacted()

### TableRowView

class TableRowView(t.Generic[ColumnIdT, T]):
    _map: t.Mapping[ColumnIdT, TableValue[T]]

    def __init__(self, map: t.Mapping[ColumnIdT, TableValue[T]]):
        self._map = map

    def __hash__(self) -> int:
        error = next((v for v in self._map.values() if isinstance(v, ErrorValue)), None)
        if error:
            raise Exception("Unexpected error value: {}".format(error)) # TODO: Make into proper exception (& test)
        return hash(frozenset((k, v) for k, v in self._map.items()))

    def __eq__(self, o: t.Any) -> bool:
        return isinstance(o, TableRowView) and self._map == t.cast(TableRowView[ColumnIdT, T], o)._map

    def __repr__(self) -> str:
        return "TableRowView({})".format(self._map)

    def column_ids(self):
        return self._map.keys()

    def values(self):
        return self._map.values()

    def get(self, column_name: ColumnIdT) -> TableValue[T]:
        return self._map.get(column_name, ErrorValue(ColumnNotFoundInRow(column_name, self)))

    def subset(self, keys: t.Collection[ColumnIdT]) -> TableRowView[ColumnIdT, T]:
        return TableRowView({ k: self.get(k) for k in keys })

    @classmethod
    def combine_views(cls, *views: TableRowView[ColumnIdT, T]) -> TableRowView[ColumnIdT, T]:
        return TableRowView(
            dict(
                v
                    for view in views
                        for v in view._map.items() 
            )
        )

### TableData

@dataclass(frozen=True)
class TableData(t.Generic[ColumnIdT, T]):
    column_ids: t.Tuple[ColumnIdT, ...]
    rows: t.Tuple[TableRowView[ColumnIdT, T], ...] # rows x columns

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.column_ids) + "\n"
        for row in self.rows:
            result += " | ".join(repr(row.get(c)) for c in self.column_ids) + "\n"
        return result