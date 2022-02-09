from .import downloadblob
from .import instrumentconfig

def to_variable_info(entry: downloadblob.SchemaEntry) -> instrumentconfig.VariableInfo:
    return instrumentconfig.VariableInfo(
        variable_id = entry.variable_id,
        type = entry.type,
        desc = entry.desc
    )

