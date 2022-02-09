from __future__ import annotations

from pyrsistent import PClass, field, pvector_field

# VariableInfo

class VariableInfo(PClass):
    variable_id = field(str)
    type = field(str)
    desc = field(str)

# Mutations

class AddIgnoreVariableMutation(PClass):
    variables = pvector_field(VariableInfo)

class RenameDescMutation(PClass):
    pass

InstrumentConfigMutation = AddIgnoreVariableMutation \
                         | RenameDescMutation

# Main class

class InstrumentConfig(PClass):
    instrument_id = field(str)
    version_id = field(str)
    title = field(str)
    import_variables = pvector_field(VariableInfo)
    ignore_variables = pvector_field(VariableInfo)
    history = pvector_field(InstrumentConfigMutation)

    def _mutate(self, cmd: InstrumentConfigMutation) -> InstrumentConfig:
        match cmd:
            case AddIgnoreVariableMutation(variables=variables):
                return self.set(
                    ignore_variables=self.ignore_variables + variables
                )
            case _: # Can remove in next version of pylance
                return self

    def mutate(self, cmd: InstrumentConfigMutation) -> InstrumentConfig:
        return self._mutate(cmd) \
                   .set(history=self.history.append(cmd))

