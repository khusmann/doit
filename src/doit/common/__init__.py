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