from __future__ import annotations

from pyrsistent import PClass, field, pvector_field, pmap_field

from .downloadblob import TypeInfo

# VariableInfo

class VariableInfo(PClass):
    variable_id = field(str)
    rename_to = field(str)
    type = field(TypeInfo)
    description = field(str)

# Mutations

class InstrumentConfigMutation(PClass):
    pass

class AddIgnoreVariableMutation(InstrumentConfigMutation):
    variables = pvector_field(VariableInfo)

class RenameDescMutation(InstrumentConfigMutation):
    pass

# Main class

class InstrumentConfig(PClass):
    instrument_id = field(str)
    version_id = field(str)
    title = field(str)
    uri = field(str)
    import_variables = pmap_field(str, VariableInfo)
    ignore_variables = pmap_field(str, VariableInfo)
    history = pvector_field(InstrumentConfigMutation)

    def _mutate(self, cmd: InstrumentConfigMutation) -> InstrumentConfig:
        match cmd:
            case AddIgnoreVariableMutation(variables=variables):
                return self.set(
                    ignore_variables=self.ignore_variables.update({ v.variable_id: v for v in variables }) # type: ignore
                )
            case _: # Can remove in next version of pylance
                return self

    def mutate(self, cmd: InstrumentConfigMutation) -> InstrumentConfig:
        return self._mutate(cmd) \
                   .set(history=self.history.append(cmd))

