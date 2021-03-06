import typing as t
_T = t.TypeVar("_T")
class tqdm(t.Iterator[_T], t.Generic[_T]):
    n: int
    def __init__(self, iterable: t.Iterable[_T] = ..., *args: t.Any, **kwargs: t.Any) -> None: ...
    def update(self, n: int = ...) -> None: ...
    def close(self) -> None: ...
def trange(*args: t.Any, **kwargs: t.Any) -> t.Iterator[int]: ...