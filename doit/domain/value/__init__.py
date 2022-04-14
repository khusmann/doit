from __future__ import annotations

from .common import *
from .unsafetable import *
from .studyspec import *
from .sanitizer import *

import yaml

# This dumper makes dictionary keys dump in order. This is so the
# ordering of keys in the stubs of instrument yamls take the same
# order that they are defined in the InstrumentSpec object.
def ordered_dict_dumper(dumper: yaml.Dumper, data: t.Dict[t.Any, t.Any]):
    return dumper.represent_dict(data.items())

def tuple_dumper(dumper: yaml.Dumper, tuple: t.Tuple[t.Any, ...]):
    return dumper.represent_list(tuple)

yaml.add_representer(dict, ordered_dict_dumper)
yaml.add_representer(tuple, tuple_dumper)