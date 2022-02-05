from pyrsistent import PRecord, field, pvector_field

class InstrumentVersion(PRecord):
    name = field(str)
    uri = field(str)

class Instrument(PRecord):
    name = field(str)
    long_name = field(str)
    description = field(str)
    versions = pvector_field(InstrumentVersion)

class Study(PRecord):
    instruments = pvector_field(Instrument)