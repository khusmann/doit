from __future__ import annotations

from pyrsistent import PRecord, field, pvector_field, pvector

from . import value

class Sanitizer(PRecord):
    instrument_id = field(str)
    version_id = field(str)
    variable_id = field(str)
    data = pvector_field((str, str | None))
    history = pvector_field(value.SanitizerMutation)

    def _mutate(self, cmd: value.SanitizerMutation) -> Sanitizer:
        match cmd:
            case value.AddValuesMutation(new_values=new_values):
                return self.set(
                    data=self.data + pvector([(i, None) for i in new_values]),
                    modified=True,
                )
            case _: # Can remove in next version of pylance
                return self