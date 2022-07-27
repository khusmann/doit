import typing as t

from ..common import ImmutableBaseModel

class LookupSanitizerItemSpec(ImmutableBaseModel):
    unsafe: t.Optional[t.Union[str, t.Tuple[str, ...]]]
    safe: t.Optional[t.Union[str, t.Tuple[str, ...]]]

class LookupSanitizerSpec(ImmutableBaseModel):
    source: str
    prompt: str
    remote_id: str
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[LookupSanitizerItemSpec, ...]

class MultiLookupSanitizerSpec(ImmutableBaseModel):
    sources: t.Mapping[str, t.Tuple[str, ...]]
    new_ids: t.Tuple[str, ...]
    action: t.Literal['sanitize']
    sanitizer: t.Tuple[LookupSanitizerItemSpec, ...]

class IdentitySanitizerSpec(ImmutableBaseModel):
    source: str
    prompt: str
    remote_id: str
    action: t.Literal['bless']

class OmitSanitizerSpec(ImmutableBaseModel):
    source: str
    prompt: str
    remote_id: str
    action: t.Literal['omit']

SanitizerSpec = t.Union[
    IdentitySanitizerSpec,
    OmitSanitizerSpec,
    MultiLookupSanitizerSpec,
    LookupSanitizerSpec,
]