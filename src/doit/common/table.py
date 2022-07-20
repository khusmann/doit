from __future__ import annotations
import typing as t
from collections import abc
import traceback
from dataclasses import dataclass

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

SingleT = t.TypeVar('SingleT', int, str, float)
SingleP = t.TypeVar('SingleP', int, str, float)

UnionSingleT = t.TypeVar('UnionSingleT', bound=int | str | float)

ColumnIdT = t.TypeVar('ColumnIdT')
ColumnIdP = t.TypeVar('ColumnIdP')

### TableValue
class Some(t.Generic[T]):
    value: T
    def __init__(self, value: T):
        self.value=value
    def __repr__(self):
        return "Some({})".format(self.value)
    def __hash__(self):
        return hash(self.value)
    def __eq__(self, o: t.Any):
        return isinstance(o, Some) and self.value == t.cast(Some[T], o).value

    def bind(self, fn: t.Callable[[T], TableValue[P]]) -> TableValue[P]:
        return fn(self.value)
    def map(self, fn: t.Callable[[T], P]) -> TableValue[P]:
        return Some(fn(self.value))
    def assert_type(self, value_type: t.Type[UnionSingleT]) -> TableValue[UnionSingleT]:
        return Some(self.value) if isinstance(self.value, value_type) else ErrorValue(IncorrectType(self.value))
    def assert_type_seq(self, value_type: t.Type[UnionSingleT]) -> TableValue[t.Sequence[UnionSingleT]]:
        if isinstance(self.value, abc.Sequence):
            multivalue = t.cast(t.Sequence[UnionSingleT], self.value)
            if all(isinstance(i, value_type) for i in multivalue):
                return Some(multivalue) 
        return ErrorValue(IncorrectType(self.value))

class Omitted:
    def __hash__(self):
        return hash(())
    def __eq__(self, o: t.Any):
        return type(self) == type(o)
    def bind(self, fn: t.Callable[[t.Any], TableValue[P]]) -> TableValue[P]:
        return self
    def map(self, fn: t.Callable[[t.Any], P]) -> TableValue[P]:
        return self
    def assert_type(self, value_type: t.Type[UnionSingleT]) -> TableValue[UnionSingleT]:
        return self
    def assert_type_seq(self, value_type: t.Type[UnionSingleT]) -> TableValue[t.Sequence[UnionSingleT]]:
        return self

class Redacted:
    def __hash__(self):
        return hash(())
    def __eq__(self, o: t.Any):
        return type(self) == type(o)
    def bind(self, fn: t.Callable[[t.Any], TableValue[P]]) -> TableValue[P]:
        return self
    def map(self, fn: t.Callable[[t.Any], P]) -> TableValue[P]:
        return self
    def assert_type(self, value_type: t.Type[UnionSingleT]) -> TableValue[UnionSingleT]:
        return self
    def assert_type_seq(self, value_type: t.Type[UnionSingleT]) -> TableValue[t.Sequence[UnionSingleT]]:
        return self

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

class ValuesAlreadyExistInRow(t.NamedTuple):
    row: t.Any
    new_row: t.Any

ErrorReason = ColumnNotFoundInRow | LookupSanitizerMiss | IncorrectType | MissingCode | ValuesAlreadyExistInRow

class ErrorValue:
    stack: traceback.StackSummary
    reason: ErrorReason

    def __init__(self, reason: ErrorReason):
        self.stack = traceback.extract_stack()
        self.reason = reason

    def __repr__(self):
        return "{}".format(str(self.reason))

    def __hash__(self):
        return hash(type(self.reason))

    def __eq__(self, o: t.Any):
        return isinstance(o, ErrorValue) and self.reason == o.reason

    @property
    def traceback(self):
        return "".join(traceback.format_list(self.stack))

    def bind(self, fn: t.Callable[[t.Any], TableValue[P]]) -> TableValue[P]:
        return self
    def map(self, fn: t.Callable[[t.Any], P]) -> TableValue[P]:
        return self
    def assert_type(self, value_type: t.Type[UnionSingleT]) -> TableValue[UnionSingleT]:
        return self
    def assert_type_seq(self, value_type: t.Type[UnionSingleT]) -> TableValue[t.Sequence[UnionSingleT]]:
        return self

TableValue = Some[T] | Omitted | Redacted | ErrorValue

def value_if_none_fn(none_value: TableValue[T]) -> t.Callable[[T | None], TableValue[T]]:
    return lambda x: none_value if x is None else Some(x)

def value_if_none_fn_seq(none_value: TableValue[t.Sequence[T]]) -> t.Callable[[t.Sequence[T | None]], TableValue[t.Sequence[T]]]:
    return lambda x: none_value if any(i is None for i in x) else t.cast(TableValue[t.Sequence[T]], Some(x))

def lookup_fn(map: t.Mapping[SingleT, SingleP]) -> t.Callable[[SingleT], TableValue[SingleP]]:
    return lambda x: value_if_none_fn(ErrorValue(MissingCode(x, map)))(map.get(x)) 

def lookup_fn_seq(map: t.Mapping[SingleT, SingleP]) -> t.Callable[[t.Sequence[SingleT]], TableValue[t.Sequence[SingleP]]]:
    return lambda x: value_if_none_fn_seq(ErrorValue(MissingCode(x, map)))(tuple(map.get(i) for i in x))

def cast_fn(to_type: t.Type[SingleP]) -> t.Callable[[SingleT], TableValue[SingleP]]:
    def inner(x: SingleT):
        try:
            return Some(to_type(x))
        except:
            return ErrorValue(IncorrectType(x))
    return inner

def cast_fn_seq(to_type: t.Type[SingleP]) -> t.Callable[[t.Sequence[SingleT]], TableValue[t.Sequence[SingleP]]]:
    def inner(x: t.Sequence[SingleT]):
        try:
            result: TableValue[t.Sequence[SingleP]] = Some(tuple(to_type(i) for i in x))
            return result
        except:
            return ErrorValue(IncorrectType(x))
    return inner

### TableRowView

class TableRowView(t.Generic[ColumnIdT]):
    _map: t.Mapping[ColumnIdT, TableValue[t.Any]]

    def __init__(self, items: t.Iterable[t.Tuple[ColumnIdT, TableValue[t.Any]]]):
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

    def items(self):
        return self._map.items()

    def get(self, column_name: ColumnIdT) -> TableValue[t.Any]:
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
            column_ids=self.column_ids, # TODO: Mistake? not subsetting column_ids?
            rows=tuple(row.subset(keys) for row in self.rows)
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.column_ids) + "\n"
        for row in self.rows:
            result += " | ".join(repr(row.get(c)) for c in self.column_ids) + "\n"
        return result

# Table error handling. TODO: Move somewhere else?

class TableErrorReportItem(t.NamedTuple):
    table_name: str
    column_name: str
    error_value: ErrorValue

TableErrorReport = t.Set[TableErrorReportItem]

import io

def write_error_report(f: io.TextIOBase, report: TableErrorReport, debug: bool = False):
    import csv
    writer = csv.writer(f)

    writer.writerow(("table_name", "column_name", "error"))

    for item in sorted(report, key=lambda x: x.table_name + x.column_name + x.error_value.reason.__class__.__name__):
        row = (item.table_name, item.column_name)
        reason = item.error_value.reason
        match reason:
            case MissingCode():
                row += ("MissingCode", reason.value, reason.codes)
            case ColumnNotFoundInRow():
                row += ("ColumnNotFoundInRow", reason.missing_column, reason.row)
            case LookupSanitizerMiss():
                row += ("LookupSanitizerMiss", reason.lookup, reason.sanitizer_map)
            case IncorrectType():
                row += ("IncorrectType", reason.value, type(reason.value))
            case ValuesAlreadyExistInRow():
                row += ("ValuesAlreadyExistInRow", reason.row, reason.new_row)
        if debug:
            row += (item.error_value.traceback,)
        
        writer.writerow(row)