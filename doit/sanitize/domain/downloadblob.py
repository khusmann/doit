from typing import OrderedDict
from pydantic import BaseModel

from .schema import SchemaEntry

class VariableData(BaseModel):
    info: SchemaEntry
    data: list[str]

class Blob(BaseModel):
    uri: str
    instrument_id: str
    version_id: str
    variables: OrderedDict[str, VariableData]

