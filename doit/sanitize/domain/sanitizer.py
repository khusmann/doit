from __future__ import annotations
from collections.abc import Mapping
from pyrsistent import PClass, field, pvector_field, pvector

# Mutations

class AddValuesMutation(PClass):
    values = pvector_field(str)

SanitizerMutation = AddValuesMutation

# Matching field
class MatcherField(PClass):
    key = field(str)
    value = field(str | None)

# Sanitizer class

class Sanitizer(PClass):
    instrument_id = field(str)
    version_id = field(str)
    variable_id = field(str)
    data = pvector_field(MatcherField) # Restriction: No duplicate keys!
    history = pvector_field(SanitizerMutation)

    def data_map(self) -> Mapping[str, str]:
        return {d.key: d.value for d in self.data}

    def _mutate(self, cmd: SanitizerMutation) -> Sanitizer:
        match cmd:
            case AddValuesMutation(values=values):
                return self.set(
                    data=self.data + pvector([MatcherField(key=i) for i in values]),
                    modified=True,
                )
            case _: # Can remove in next version of pylance
                return self

    def mutate(self, cmd: SanitizerMutation) -> Sanitizer:
        return self._mutate(cmd) \
                   .set(history=self.history.append(cmd))