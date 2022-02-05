from pyrsistent import PRecord, field, pvector_field

class InstrumentVersion(PRecord):
    name = field(type=str)
    uri = field(type=str)

class Instrument(PRecord):
    name = field(type=str)
    long_name = field(type=str)
    description = field(type=str)
    versions = pvector_field(InstrumentVersion)

class Study(PRecord):
    instruments = pvector_field(Instrument)