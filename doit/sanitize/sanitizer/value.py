from pyrsistent import PRecord, pvector_field

# Mutations

class AddValuesMutation(PRecord):
    new_values = pvector_field(str)

SanitizerMutation = AddValuesMutation