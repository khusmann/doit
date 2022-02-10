from __future__ import annotations
from typing import OrderedDict
from pydantic import BaseModel

# Mutations

class SanitizerMutation(BaseModel):
    pass

class AddValuesMutation(SanitizerMutation):
    values: set[str]

# Sanitizer class

class Sanitizer(BaseModel):
    instrument_id: str
    version_id: str
    variable_id: str
    data_map: OrderedDict[str, str | None]
    history: list[SanitizerMutation]

    def _mutate(self, cmd: SanitizerMutation):
        match cmd:
            case AddValuesMutation(values=values):
                self.data_map.update({ v: None for v in values })
            case _: # Can remove in next version of pylance
                pass

    def mutate(self, cmd: SanitizerMutation) -> Sanitizer:
        result = self.copy(deep=True)
        cmd = cmd.copy(deep=True)
        result._mutate(cmd)
        result.history.append(cmd)
        return result