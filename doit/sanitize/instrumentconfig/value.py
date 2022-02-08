from pyrsistent import PRecord, field, pvector_field

# VariableInfo

class VariableInfo(PRecord):
    variable_id = field(str)
    type = field(str)
    desc = field(str)

# Mutations

class AddIgnoreVariableMutation(PRecord):
    variables = pvector_field(VariableInfo)

## class RenameDescMutation(PRecord):

InstrumentConfigMutation = AddIgnoreVariableMutation

