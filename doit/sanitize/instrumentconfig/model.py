from __future__ import annotations

from pyrsistent import PRecord, field, pvector_field

from . import value

class InstrumentConfig(PRecord):
    instrument_id = field(str)
    version_id = field(str)
    title = field(str)
    import_variables = pvector_field(value.VariableInfo)
    ignore_variables = pvector_field(value.VariableInfo)
    history = pvector_field(value.InstrumentConfigMutation)

    def _mutate(self, cmd: value.InstrumentConfigMutation) -> InstrumentConfig:
        match cmd:
            case value.AddIgnoreVariableMutation(variable=variable):
                return self.set(
                    ignore_variables=self.ignore_variables.append(variable)
                )
            case _: # Can remove in next version of pylance
                return self

    def mutate(self, cmd: value.InstrumentConfigMutation) -> InstrumentConfig:
        return self._reducer(cmd) \
                   .set(history=self.history.append(cmd))

