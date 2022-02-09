from pyrsistent import PClass, field, pmap_field, pvector_field

class SchemaEntry(PClass):
    variable_id = field(str)
    type = field(str)
    desc = field(str)
    # Include other things?

class VariableData(PClass):
    schema = field(SchemaEntry)
    data = pvector_field(str)

class Blob(PClass):
    uri = field(str)
    instrument_id = field(str)
    version_id = field(str)
    variables = pmap_field(str, VariableData)

class Schema(PClass):
    uri = field(str)
    instrument_id = field(str)
    version_id = field(str)
    entries = pmap_field(str, SchemaEntry)