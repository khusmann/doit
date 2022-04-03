from __future__ import annotations
import typing as t
from pydantic import BaseModel
from pydantic.generics import GenericModel

class ImmutableBaseModel(BaseModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

class ImmutableBaseModelOrm(ImmutableBaseModel):
    class Config(ImmutableBaseModel.Config):
        orm_mode = True

class ImmutableGenericModel(GenericModel):
    class Config:
        allow_mutation=False
    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))

class Uri(str):
    def as_tuple(self) -> t.Tuple[str, ...]:
        return tuple(self.split('.'))

    @classmethod
    def from_tuple(cls, v: t.Tuple[str, ...]):
        return cls('.'.join(v))

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: t.Any):
        if not isinstance(v, str):
            raise TypeError('string required')
        return cls(v)

    def __truediv__(self, v: str | Uri):
        match v:
            case Uri():
                return self.from_tuple((*self.as_tuple(), *v.as_tuple()))
            case str():
                return self.from_tuple((*self.as_tuple(), v))

