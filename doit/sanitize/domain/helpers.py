from . import downloadblob
from . import instrumentconfig
from . import helpers

def to_variable_info(entry: downloadblob.SchemaEntry) -> instrumentconfig.VariableInfo:
    return instrumentconfig.VariableInfo(
        variable_id = entry.id,
        type = entry.type,
        description = entry.description,
        rename_to = entry.rename_to
    )

def new_instrument_config(instrument_id: str, version_id: str, schema: downloadblob.Schema):
    return instrumentconfig.InstrumentConfig(
        instrument_id=instrument_id,
        version_id=version_id,
        title=schema.title,
        uri=schema.uri,
        import_variables={},
        ignore_variables={ value.id: helpers.to_variable_info(value) for (_, value) in schema.entries.items() },
    )
