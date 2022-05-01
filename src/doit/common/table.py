from __future__ import annotations
import typing as t
import traceback
from dataclasses import dataclass

from functools import reduce

### Value types

OrdinalValue = t.NewType('OrdinalValue', int)
OrdinalLabel = t.NewType('OrdinalValue', str)
OrdinalTag = t.NewType('OrdinalTag', str)

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
    value: t.Any

class Multi(t.NamedTuple):
    values: t.Tuple[t.Any, ...]

class Omitted(t.NamedTuple):
    pass

class Redacted(t.NamedTuple):
    pass

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

TableValue = Some | Multi | Omitted | Redacted | ErrorValue

def omitted_if_empty(value: t.Optional[t.Any]) -> TableValue:
    return Some(value) if value else Omitted()

def redacted_if_empty(value: t.Optional[t.Any]) -> TableValue:
    return Some(value) if value else Redacted()

def tv_combine(tv1: TableValue, tv2: TableValue) -> TableValue:
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

def tv_bind(
    tv: TableValue,
    fn: t.Callable[[T], TableValue],
    type: t.Type[T],
) -> TableValue:
    match tv:
        case Multi(values=values) if all(isinstance(i, type) for i in values):
            return reduce(tv_combine, (fn(i) for i in values))
        case Multi(values=values):
            return ErrorValue(IncorrectType(values))
        case Some(value=value) if isinstance(value, type):
            return fn(value)
        case Some(value=value):
            return ErrorValue(IncorrectType(value))
        case Redacted() | Omitted() | ErrorValue():
            return tv

def tv_lookup(tv: TableValue, m: t.Mapping[T, t.Any], type: t.Type[T]):
    return tv_bind(tv, lambda v: Some(m[v]) if v in m else ErrorValue(MissingCode(v, m)), type)

def tv_lookup_with_default(tv: TableValue, m: t.Mapping[T, t.Any], type: t.Type[T]):
    return tv_bind(tv, lambda v: Some(m.get(v, v)), type)

### TableRowView

class TableRowView(t.Generic[ColumnIdT]):
    _map: t.Mapping[ColumnIdT, TableValue]

    def __init__(self, map: t.Mapping[ColumnIdT, TableValue]):
        self._map = map

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
        return TableRowView({ k: self.get(k) for k in keys })

    def has_some(self):
        return any(isinstance(i, Some) for i in self._map.values())

    @classmethod
    def combine_views(cls, *views: TableRowView[ColumnIdT]) -> TableRowView[ColumnIdT]:
        return TableRowView(
            dict(
                v
                    for view in views
                        for v in view._map.items() 
            )
        )

    @classmethod
    def from_pair(cls, id: ColumnIdT, tv: TableValue):
        return cls({ id: tv })

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