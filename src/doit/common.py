from __future__ import annotations
import typing as t
import traceback
from dataclasses import dataclass
from pydantic import BaseModel

class ImmutableBaseModel(BaseModel):
    class Config:
        frozen=True
        smart_union = True

### Error types

class EmptyHeaderError(ValueError):
    pass

class DuplicateHeaderError(ValueError):
    pass

class EmptySanitizerKeyError(ValueError):
    pass

### Table types

T = t.TypeVar('T')
P = t.TypeVar("P")

MissingReason = t.Literal['omitted', 'redacted']
ErrorReason = t.Literal['unknown_column', 'missing_sanitizer']

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
    def __eq__(self, o: t.Any):
        return isinstance(o, Error) and self.reason == o.reason and self.info == o.info
    def is_type(self, _: t.Any) -> t.TypeGuard[Error]:
        return True

ColumnIdT = t.TypeVar('ColumnIdT')
ColumnIdP = t.TypeVar('ColumnIdP')
TableValue = Some[T] | Missing | Error

@dataclass(frozen=True)
class RowViewHash(t.Generic[ColumnIdT, T]):
    value: int

MaybeRowViewHash = Some[RowViewHash[ColumnIdT, T]] | Error

@dataclass(frozen=True)
class TableRowView(t.Generic[ColumnIdT, T]):
    _map: t.Mapping[ColumnIdT, TableValue[T]]

    def keys(self):
        return self._map.keys()

    def values(self):
        return self._map.values()

    def get(self, column_name: ColumnIdT) -> TableValue[T]:
        return self._map.get(column_name, Error('unknown_column', column_name))

    def subset(self, keys: t.Collection[ColumnIdT]) -> TableRowView[ColumnIdT, T]:
        return TableRowView({ k: self.get(k) for k in keys })

    def subset_type(self, value_type: t.Type[P]) -> TableRowView[ColumnIdT, P]:
        return TableRowView({ k: v for k, v in self._map.items() if v.is_type(value_type)})

    def hash(self) -> MaybeRowViewHash[ColumnIdT, T]:
        error = next((v for v in self._map.values() if isinstance(v, Error)), None)
        return error if error else Some(RowViewHash(hash(frozenset((k, v) for k, v in self._map.items()))))

    def hash_or_die(self) -> RowViewHash[ColumnIdT, T]:
        val = self.hash()
        match val:
            case Some():
                return val.value
            case Error():
                raise Exception("Unexpected error value: {}".format(val))

    def bless_ids(self, fn: t.Callable[[ColumnIdT], ColumnIdP]) -> TableRowView[ColumnIdP, T]:
        return TableRowView({ fn(k): v for k, v in self._map.items()})

    @classmethod
    def combine_views(cls, *views: TableRowView[ColumnIdT, T]) -> TableRowView[ColumnIdT, T]:
        return TableRowView(
            dict(
                v
                    for view in views
                        for v in view._map.items() 
            )
        )

@dataclass(frozen=True)
class TableData(t.Generic[ColumnIdT, T]):
    columns_ids: t.Tuple[ColumnIdT, ...]
    rows: t.Tuple[TableRowView[ColumnIdT, T], ...] # rows x columns

    @property
    def str_rows(self):
        return (
            row.subset_type(str) for row in self.rows
        )

    def __repr__(self):
        result = " | ".join(repr(c) for c in self.columns_ids) + "\n"
        for row in self.rows:
            result += " | ".join(str(row.get(c)) for c in self.columns_ids) + "\n"
        return result

