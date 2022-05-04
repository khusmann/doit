import typing as t
from pydantic import BaseModel
from pydantic.generics import GenericModel

class ImmutableBaseModel(BaseModel):
    class Config:
        frozen=True
        smart_union = True

class ImmutableGenericModel(GenericModel):
    class Config:
        frozen=True
        smart_union = True

def assert_never(value: t.NoReturn) -> t.NoReturn:
    assert False, 'This code should never be reached, got: {0}'.format(value)