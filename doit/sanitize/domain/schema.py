from typing import OrderedDict
from pydantic import BaseModel

class TypeInfo(BaseModel):
    dtype = "unknown"

class CategoryTypeInfo(TypeInfo):
    dtype = "category"
    categories: OrderedDict[str, str]

class SchemaEntry(BaseModel):
    variable_id: str
    rename_to: str
    type: TypeInfo
    description: str
    # Include other things?

class Schema(BaseModel):
    title: str
    uri: str
    entries:  OrderedDict[str, SchemaEntry]

