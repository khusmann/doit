from typing import OrderedDict

from .schema import Schema
from .downloadblob import Blob
from .instrumentconfig import InstrumentConfig, AddIgnoreVariableMutation
from .sanitizer import Sanitizer, AddValuesMutation

def update_instrument_config(i: InstrumentConfig, s: Schema ) -> InstrumentConfig:
    existing_keys = list(i.ignore_variables.keys()) + list(i.import_variables.keys())
    new_keys = set(s.entries.keys()) - set(existing_keys)
    mutation = AddIgnoreVariableMutation(
        variables = OrderedDict({key: s.entries[key] for key in new_keys})
    )
    return i.mutate(mutation)

def update_sanitizer(s: Sanitizer, d: Blob) -> Sanitizer:
    assert s.instrument_id == d.instrument_id
    assert s.version_id == d.version_id
    all_unique_values = d.variables[s.variable_id].data
    mutation = AddValuesMutation(
        values = set(all_unique_values) - set(s.data_map.keys())
    )
    return s.mutate(mutation)

