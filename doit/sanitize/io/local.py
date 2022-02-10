import typing as t
from collections import OrderedDict
import yaml
from ..domain.instrumentconfig import InstrumentConfig
from ..domain.schema import SchemaEntry, TypeInfo, CategoryTypeInfo

def ordered_dict_dumper(dumper: yaml.Dumper, data: OrderedDict[str, t.Any]):
    return dumper.represent_mapping('tag:yaml.org,2002:map', data.items())


def instrumentconfig_dumper(dumper: yaml.Dumper, instrument: InstrumentConfig):
    return dumper.represent_mapping('tag:yaml.org,2002:map', [
        ('title', instrument.title),
        ('uri', instrument.uri),
        ('import_variables', instrument.import_variables),
        ('ignore_variables', instrument.ignore_variables),
    ])

def schemaentry_dumper(dumper: yaml.Dumper, schemaentry: SchemaEntry):
    return dumper.represent_mapping('tag:yaml.org,2002:map', [
        ('rename_to', schemaentry.rename_to),
        ('description', schemaentry.description),
        ('type', schemaentry.type),
    ])

def categorytypeinfo_dumper(dumper: yaml.Dumper, categorytypeinfo: CategoryTypeInfo):
    return dumper.represent_mapping('tag:yaml.org,2002:map', categorytypeinfo.categories.items())

def typeinfo_dumper(dumper: yaml.Dumper, typeinfo: TypeInfo):
    return dumper.represent_scalar('tag:yaml.org,2002:str', typeinfo.dtype) # type: ignore

yaml.add_representer(OrderedDict, ordered_dict_dumper)
yaml.add_representer(InstrumentConfig, instrumentconfig_dumper)
yaml.add_representer(SchemaEntry, schemaentry_dumper)
yaml.add_representer(TypeInfo, typeinfo_dumper)
yaml.add_representer(CategoryTypeInfo, categorytypeinfo_dumper)

class InstrumentConfigRepoImpl:
    def get_instrument_config_filename(self, instrument_id: str, version_id: str):
        return "./instruments/{instrument}/{instrument}-{version}.yaml".format(
            instrument=instrument_id,
            version=version_id,
        )
    def save(self, instrument: InstrumentConfig):
        with open(self.get_instrument_config_filename(instrument.instrument_id, instrument.version_id), 'w') as f:
            yaml.dump(instrument, f) # type: ignore
            
    def load(self, instrument_id: str, version_id: str):
        with open(self.get_instrument_config_filename(instrument_id, version_id), 'r') as f:
            return InstrumentConfig(yaml.load(f)) # type: ignore