from __future__ import annotations

from .common import *

from .table import *

from .study import *

import yaml

def ordered_dict_dumper(dumper: yaml.Dumper, data: t.Dict[t.Any, t.Any]):
    return dumper.represent_dict(data.items())

def tuple_dumper(dumper: yaml.Dumper, tuple: t.Tuple[t.Any, ...]):
    return dumper.represent_list(tuple)

yaml.add_representer(dict, ordered_dict_dumper)
yaml.add_representer(tuple, tuple_dumper)

