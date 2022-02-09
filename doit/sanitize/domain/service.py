from pyrsistent import pset

from . import downloadblob
from . import instrumentconfig
from . import sanitizer
from . import helpers

def update_instrument_config(i: instrumentconfig.InstrumentConfig, s: downloadblob.Schema ) -> instrumentconfig.InstrumentConfig:
    schema_var_info = pset(map(helpers.to_variable_info, s.entries.values()))
    mutation = instrumentconfig.AddIgnoreVariableMutation(
        variables = pset(i.ignore_variables.keys()).update(i.import_variables.keys()) - schema_var_info
    )
    return i.mutate(mutation)

def update_sanitizer(s: sanitizer.Sanitizer, d: downloadblob.Blob) -> sanitizer.Sanitizer:
    assert s.instrument_id == d.instrument_id
    assert s.version_id == d.version_id
    all_unique_values = pset(d.variables[s.variable_id].data)
    mutation = sanitizer.AddValuesMutation(
        values = pset(all_unique_values - s.data_map().keys())
    )
    return s.mutate(mutation)

