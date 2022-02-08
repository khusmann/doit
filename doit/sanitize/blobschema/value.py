from pyrsistent import PRecord, field

class SchemaEntry(PRecord):
    variable_id = field(str)
    type = field(str)
    desc = field(str)
    # Include other things?