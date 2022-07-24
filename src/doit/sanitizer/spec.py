import typing as t

from ..common import ImmutableBaseModel

class TableLookupSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    action: t.Literal['sanitize']
    sanitizer: str

class SanitizerItemSpec(ImmutableBaseModel):
    unsafe: str
    safe: str

class MultilineSimpleSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[SanitizerItemSpec, ...]

class SimpleSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Mapping[str, str]

class IdentitySanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['bless']

class OmitSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['omit']

SanitizerSpec = t.Union[
    TableLookupSanitizerSpec,
    IdentitySanitizerSpec,
    MultilineSimpleSanitizerSpec,
    SimpleSanitizerSpec,
    OmitSanitizerSpec,
]

class StudySanitizerSpec(ImmutableBaseModel):
    __root__: t.Mapping[str, t.Tuple[SanitizerSpec, ...]]
    def get(self, table_name: str):
        return self.__root__.get(table_name)
    def items(self):
        return self.__root__.items()