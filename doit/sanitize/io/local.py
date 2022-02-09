import typing as t
from pyrsistent import PClass
from collections import OrderedDict
import yaml
from ..domain import instrumentconfig, downloadblob

def ordered_dict_dumper(dumper: yaml.Dumper, data: OrderedDict[str, t.Any]):
    return dumper.represent_mapping('tag:yaml.org,2002:map', data.items())

yaml.add_representer(OrderedDict, ordered_dict_dumper)

def instrumentconfig_dumper(dumper: yaml.Dumper, instrument: instrumentconfig.InstrumentConfig):
    return dumper.represent_mapping('tag:yaml.org,2002:map', [
        ('title', instrument.title),
        ('uri', instrument.uri),
        ('import_variables', OrderedDict(instrument.import_variables)),
        ('ignore_variables', OrderedDict(instrument.ignore_variables)),
    ])

def variableinfo_dumper(dumper: yaml.Dumper, variableinfo: instrumentconfig.VariableInfo):
    return dumper.represent_mapping('tag:yaml.org,2002:map', [
        ('rename_to', variableinfo.rename_to),
        ('description', variableinfo.description),
        ('type', variableinfo.type),
    ])


def categorytypeinfo_dumper(dumper: yaml.Dumper, categorytypeinfo: downloadblob.CategoryTypeInfo):
    return dumper.represent_mapping('tag:yaml.org,2002:map', OrderedDict(categorytypeinfo.categories).items())

def typeinfo_dumper(dumper: yaml.Dumper, typeinfo: downloadblob.TypeInfo):
    return dumper.represent_scalar('tag:yaml.org,2002:str', typeinfo.dtype) # type: ignore

yaml.add_representer(instrumentconfig.InstrumentConfig, instrumentconfig_dumper)
yaml.add_representer(instrumentconfig.VariableInfo, variableinfo_dumper)
yaml.add_representer(downloadblob.TypeInfo, typeinfo_dumper)
yaml.add_representer(downloadblob.CategoryTypeInfo, categorytypeinfo_dumper)

class InstrumentConfigRepoImpl(PClass):
    def instrument_config_to_filename(self, instrument: instrumentconfig.InstrumentConfig):
        return "./instruments/{instrument}/{instrument}-{version}.yaml".format(
            instrument=instrument.instrument_id,
            version=instrument.version_id,
        )
    def save(self, instrument: instrumentconfig.InstrumentConfig):
        with open(self.instrument_config_to_filename(instrument), 'w') as f:
            yaml.dump(instrument, f) # type: ignore
    def load(self):
        pass