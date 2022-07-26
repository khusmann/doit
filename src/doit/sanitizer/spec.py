import typing as t

from ..common import ImmutableBaseModel

class MultiSanitizerSpec(ImmutableBaseModel):
    prompt: str
    src_remote_ids: t.Tuple[str, ...]
    dst_remote_ids: t.Tuple[str, ...]
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[t.Mapping[str, t.Optional[str]], ...]

class IdentitySanitizerSpec(ImmutableBaseModel):
    prompt: str
    remote_id: str
    action: t.Literal['bless']

class OmitSanitizerSpec(ImmutableBaseModel):
    prompt: str
    remote_id: str
    action: t.Literal['omit']

SanitizerSpec = t.Union[
    IdentitySanitizerSpec,
    OmitSanitizerSpec,
    MultiSanitizerSpec,
]

class StudySanitizerSpec(ImmutableBaseModel):
    __root__: t.Mapping[str, t.Tuple[SanitizerSpec, ...]]
    def get(self, table_name: str):
        return self.__root__.get(table_name)
    def items(self):
        return self.__root__.items()