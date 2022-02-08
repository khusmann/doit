from pyrsistent import PRecord, field, pset_field

from . import value


class BlobSchema(PRecord):
    blob_uri = field(str)
    schema = pset_field(value.SchemaEntry)