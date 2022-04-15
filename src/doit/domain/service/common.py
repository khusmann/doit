from __future__ import annotations
import typing as t
from functools import reduce

T = t.TypeVar('T')
P = t.TypeVar('P')
Q = t.TypeVar('Q')

def lift_none(func: t.Callable[[T], P]) -> t.Callable[[T | None], P | None]:
    def inner(i: T | None) -> P | None:
        return None if i is None else func(i)
    return inner

def dmap(f: t.Callable[[P], Q], m: t.Mapping[T, P]) -> t.Mapping[T, Q]:
    return {
        k: f(v) for (k, v) in m.items()
    }

def merge_mappings(d: t.Sequence[t.Mapping[T, P]]) -> t.Dict[T, P]:
    return reduce(lambda acc, x: acc | x, map(lambda x: dict(x), d))

def invert_map(m: t.Mapping[T, P]) -> t.Mapping[P, t.FrozenSet[T]]:
    result: t.Mapping[P, t.FrozenSet[T]] = {}
    for k, v in m.items():
        result[v] = result.get(v, frozenset()) | {k}
    return result

