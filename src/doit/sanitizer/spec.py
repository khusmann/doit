import typing as t

from ..common import ImmutableBaseModel

class UnsafeSanitizerItemSpec(ImmutableBaseModel):
    checksum: str
    unsafe: str

class SafeSanitizerItemSpec(ImmutableBaseModel):
    checksum: str
    safe: str

class SimpleSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[t.Union[SafeSanitizerItemSpec, UnsafeSanitizerItemSpec], ...]

class MultiSanitizerSpec(ImmutableBaseModel):
    src_remote_ids: t.Tuple[str, ...]
    dst_remote_id: str
    prompt: str
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[t.Union[SafeSanitizerItemSpec, UnsafeSanitizerItemSpec], ...]

class IdentitySanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['bless']

class OmitSanitizerSpec(ImmutableBaseModel):
    remote_id: str
    prompt: str
    action: t.Literal['omit']

SanitizerSpec = t.Union[
    IdentitySanitizerSpec,
    OmitSanitizerSpec,
    MultiSanitizerSpec,
    SimpleSanitizerSpec,
]

class StudySanitizerSpec(ImmutableBaseModel):
    __root__: t.Mapping[str, t.Tuple[SanitizerSpec, ...]]
    def get(self, table_name: str):
        return self.__root__.get(table_name)
    def items(self):
        return self.__root__.items()