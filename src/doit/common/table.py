from __future__ import annotations
import typing as t
from collections import abc
import traceback
from dataclasses import dataclass

from functools import reduce

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

### TableValue

class Some(t.NamedTuple):
    value: int | str | float
    def bind(self, fn: t.Callable[[T], TableValue], type: t.Type[T]) -> TableValue:
        return _bind(self, fn, type)
    def lookup(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup(self, m, type)
    def lookup_with_default(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup_with_default(self, m, type)
 
class Multi(t.NamedTuple):
    values: t.Tuple[t.Any, ...]
    def bind(self, fn: t.Callable[[T], TableValue], type: t.Type[T]) -> TableValue:
        return _bind(self, fn, type)
    def lookup(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup(self, m, type)
    def lookup_with_default(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup_with_default(self, m, type)

class Omitted(t.NamedTuple):
    def bind(self, fn: t.Callable[[T], TableValue], type: t.Type[T]) -> TableValue:
        return _bind(self, fn, type)
    def lookup(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup(self, m, type)
    def lookup_with_default(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup_with_default(self, m, type)

class Redacted(t.NamedTuple):
    def bind(self, fn: t.Callable[[T], TableValue], type: t.Type[T]) -> TableValue:
        return _bind(self, fn, type)
    def lookup(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup(self, m, type)
    def lookup_with_default(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup_with_default(self, m, type)

class ColumnNotFoundInRow(t.NamedTuple):
    missing_column: t.Any
    row: t.Any

class LookupSanitizerMiss(t.NamedTuple):
    lookup: t.Any
    sanitizer_map: t.Any

class IncorrectType(t.NamedTuple):
    value: t.Any

class MissingCode(t.NamedTuple):
    value: t.Any
    codes: t.Any

ErrorReason = ColumnNotFoundInRow | LookupSanitizerMiss | IncorrectType | MissingCode

class ErrorValue:
    stack: traceback.StackSummary
    reason: ErrorReason

    def __init__(self, reason: ErrorReason):
        self.stack = traceback.extract_stack()
        self.reason = reason

    def __repr__(self):
        return "{}".format(str(self.reason))

    def __eq__(self, o: t.Any):
        return isinstance(o, ErrorValue) and self.reason == o.reason

    def print_traceback(self):
        print("".join(traceback.format_list(self.stack)))

    def bind(self, fn: t.Callable[[T], TableValue], type: t.Type[T]) -> TableValue:
        return _bind(self, fn, type)
    def lookup(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup(self, m, type)
    def lookup_with_default(self, m: t.Mapping[T, t.Any], type: t.Type[T]):
        return _lookup_with_default(self, m, type)

TableValue = Some | Multi | Omitted | Redacted | ErrorValue

def from_optional(value: t.Optional[int | str | float | t.Tuple[t.Any, ...]], none_value: TableValue):
    match value:
        case None:
            return none_value
        case str():
            return Some(value)
        case abc.Sequence():
            if len(value) == 0:
                return none_value
            else:
                return Multi(value)
        case _:
            return Some(value)        

def _combine(tv1: TableValue, tv2: TableValue) -> TableValue:
    if isinstance(tv1, ErrorValue):
        return tv1 
    if isinstance(tv2, ErrorValue):
        return tv2
    if isinstance(tv1, Redacted | Omitted):
        return ErrorValue(IncorrectType(tv1))
    if isinstance(tv2, Redacted | Omitted):
        return ErrorValue(IncorrectType(tv2))
    match tv1:
        case Some(value=v1):
            match tv2:
                case Some(value=v2):
                    return Multi((v1, v2))
                case Multi(values=v2s):
                    return Multi((v1, *v2s))
        case Multi(values=v1s):
            match tv2:
                case Some(value=v2):
                    return Multi((*v1s, v2))
                case Multi(values=v2s):
                    return Multi((*v1s, *v2s))

def _bind(
    tv: TableValue,
    fn: t.Callable[[T], TableValue],
    type: t.Type[T],
) -> TableValue:
    match tv:
        case Multi(values=values) if all(isinstance(i, type) for i in values):
            return reduce(_combine, (fn(i) for i in values))
        case Multi(values=values):
            return ErrorValue(IncorrectType(values))
        case Some(value=value) if isinstance(value, type):
            return fn(value)
        case Some(value=value):
            return ErrorValue(IncorrectType(value))
        case Redacted() | Omitted() | ErrorValue():
            return tv

def _lookup(tv: TableValue, m: t.Mapping[T, t.Any], type: t.Type[T]):
    return _bind(tv, lambda v: Some(m[v]) if v in m else ErrorValue(MissingCode(v, m)), type)

def _lookup_with_default(tv: TableValue, m: t.Mapping[T, t.Any], type: t.Type[T]):
    return _bind(tv, lambda v: Some(m.get(v, v)), type)

### TableRowView

class TableRowView(t.Generic[ColumnIdT]):
    _map: t.Mapping[ColumnIdT, TableValue]

    def __init__(self, items: t.Iterable[t.Tuple[ColumnIdT, TableValue]]):
        self._map = dict(items)

    def __hash__(self) -> int:
        error = next((v for v in self._map.values() if isinstance(v, ErrorValue)), None)
        if error:
            raise Exception("Unexpected error value: {}".format(error)) # TODO: Make into proper exception (& test)
        return hash(frozenset((k, v) for k, v in self._map.items()))

    def __eq__(self, o: t.Any) -> bool:
        return isinstance(o, TableRowView) and self._map == t.cast(TableRowView[ColumnIdT], o)._map

    def __repr__(self) -> str:
        return "TableRowView({})".format(self._map)

    def column_ids(self):
        return self._map.keys()

    def values(self):
        return self._map.values()

    def get(self, column_name: ColumnIdT) -> TableValue:
        return self._map.get(column_name, ErrorValue(ColumnNotFoundInRow(column_name, self)))

    def subset(self, keys: t.Collection[ColumnIdT]) -> TableRowView[ColumnIdT]:
        return TableRowView((k, self.get(k)) for k in keys)

    def has_some(self):
        return any(isinstance(i, Some) for i in self._map.values())

    @classmethod
    def combine_views(cls, *views: TableRowView[ColumnIdT]) -> TableRowView[ColumnIdT]:
        return TableRowView(
            v
                for view in views
                    for v in view._map.items() 
        )

### TableData

@dataclass(frozen=True)
class TableData(t.Generic[ColumnIdT]):
    column_ids: t.Tuple[ColumnIdT, ...]
    rows: t.Tuple[TableRowView[ColumnIdT], ...]

    def subset(self, keys: t.Collection[ColumnIdT]):
        return TableData(
            column_ids=self.column_ids,
            rows=tuple(row.subset(keys) for row in self.rows)
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.column_ids) + "\n"
        for row in self.rows:
            result += " | ".join(repr(row.get(c)) for c in self.column_ids) + "\n"
        return result