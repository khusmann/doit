from .instrumentconfig import InstrumentConfig
from .schema import Schema

def new_instrument_config(instrument_id: str, version_id: str, schema: Schema):
    return InstrumentConfig(
        instrument_id=instrument_id,
        version_id=version_id,
        title=schema.title,
        uri=schema.uri,
        ignore_variables=schema.entries,
    )
