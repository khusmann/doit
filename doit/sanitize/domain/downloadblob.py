from pyrsistent import PClass, field, pmap_field, pvector_field

class TypeInfo(PClass):
    dtype = field(str)
    needs_sanitization = True

class CategoryTypeInfo(TypeInfo):
    needs_sanitization = False
    categories = pmap_field(str, str)

class SchemaEntry(PClass):
    id = field(str)
    rename_to = field(str)
    type = field(TypeInfo)
    description = field(str)
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
    title = field(str)
    entries = pmap_field(str, SchemaEntry)