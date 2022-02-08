from pyrsistent import pset, pvector

from . import model
from . import value

from ..blobschema import model as blobschema_model
from ..blobschema import value as blobschema_value

def _schema_entry_to_variable_info(entry: blobschema_value.SchemaEntry):
    return value.VariableInfo(
        variable_id = entry.variable_id,
        type = entry.type,
        desc = entry.desc
    )

def update_instrument_config(instrument: model.InstrumentConfig, schema: blobschema_model.BlobSchema ) -> model.InstrumentConfig:
    schema_var_info = pset([_schema_entry_to_variable_info(i) for i in schema.schema])
    mutation = value.AddIgnoreVariableMutation(
        variables = pvector(pset(instrument.ignore_variables + instrument.import_variables) - schema_var_info)
    )
    return instrument.mutate(mutation)
