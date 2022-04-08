from __future__ import annotations

from .common import *
from .unsafetable import *
from .studyspec import *

import yaml

def tuple_dumper(dumper: yaml.Dumper, tuple: t.Tuple[t.Any, ...]):
    return dumper.represent_list(tuple)

yaml.add_representer(tuple, tuple_dumper)