import typing as t

from ..common import ImmutableBaseModel

class TableLookupSanitizerSpec(ImmutableBaseModel):
    action: t.Literal['sanitize']
    sanitizer: str

class SanitizerItemSpec(ImmutableBaseModel):
    unsafe: str
    safe: str

class MultilineSimpleSanitizerSpec(ImmutableBaseModel):
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[SanitizerItemSpec, ...]

class SimpleSanitizerSpec(ImmutableBaseModel):
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Mapping[str, str]

class IdentitySanitizerSpec(ImmutableBaseModel):
    prompt: str
    action: t.Literal['bless']

class OmitSanitizerSpec(ImmutableBaseModel):
    prompt: str
    action: t.Literal['omit']

SanitizerSpec = t.Union[
    TableLookupSanitizerSpec,
    IdentitySanitizerSpec,
#    SimpleSanitizerSpec,
    OmitSanitizerSpec,
    MultilineSimpleSanitizerSpec,
]

class TableSanitizerSpec(ImmutableBaseModel):
    __root__: t.Mapping[str, SanitizerSpec]
    def get(self, sanitizer_name: str):
        return self.__root__.get(sanitizer_name)
    def items(self):
        return self.__root__.items()

class StudySanitizerSpec(ImmutableBaseModel):
    __root__: t.Mapping[str, TableSanitizerSpec]
    def get(self, table_name: str):
        return self.__root__.get(table_name)
    def items(self):
        return self.__root__.items()