from __future__ import annotations

from typing import OrderedDict
from pydantic import BaseModel

from .schema import SchemaEntry

# Mutations

class InstrumentConfigMutation(BaseModel):
    pass

class AddIgnoreVariableMutation(InstrumentConfigMutation):
    variables: OrderedDict[str, SchemaEntry]

class RenameDescMutation(InstrumentConfigMutation):
    pass

# Main class

class InstrumentConfig(BaseModel):
    instrument_id: str
    version_id: str
    title: str
    uri: str
    import_variables: OrderedDict[str, SchemaEntry] = OrderedDict({})
    ignore_variables: OrderedDict[str, SchemaEntry] = OrderedDict({})
    history: list[InstrumentConfigMutation] = []

    def _mutate(self, cmd: InstrumentConfigMutation):
        match cmd:
            case AddIgnoreVariableMutation(variables=variables):
                self.ignore_variables.update(variables)
            case _: # Can remove in next version of pylance
                return self

    def mutate(self, cmd: InstrumentConfigMutation) -> InstrumentConfig:
        result = self.copy(deep=True)
        cmd = cmd.copy(deep=True)
        result._mutate(cmd)
        result.history.append(cmd)
        return result