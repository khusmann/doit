import typing as t

from sqlalchemy import (
    select,
)

from sqlalchemy.orm import (
    Session,
)

from sqlalchemy.engine import Engine

def str_or_none(val: t.Any):
    return None if val is None else str(val)

T = t.TypeVar('T')

class SessionWrapper(Session):
    def __init__(self, engine: Engine):
        super().__init__(engine)

    def get_by_name(self, type: t.Type[T], name: str) -> T:
        entry: T | None = self.execute( # type: ignore
            select(type).filter_by(name=name)
        ).scalars().one_or_none()

        if entry is None:
            raise Exception("Error: Entry named {} not found".format(name))

        return entry

    def get_all(self, type: t.Type[T]) -> t.Sequence[T]:
        return self.execute( # type: ignore
            select(type)
        ).scalars().all()

