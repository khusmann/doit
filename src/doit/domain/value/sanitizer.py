import typing as t
from datetime import datetime
from .common import *

SanitizerName = t.NewType('SanitizerName', str)

class Sanitizer(ImmutableBaseModel):
    name: SanitizerName
    instrument_name: InstrumentName
    last_modified: t.Optional[datetime]
    checksum: t.Optional[str]
    columns: t.Mapping[SourceColumnName, t.Tuple[t.Optional[str], ...]]

    def num_empty(self):
        return sum([
                int(v == "")
                    for c in self.columns.values()
                        for v in c
        ])

class SanitizerUpdate(ImmutableBaseModel):
    name: SanitizerName
    instrument_name: InstrumentName
    columns: t.Mapping[SourceColumnName, t.Tuple[t.Optional[str], ...]]