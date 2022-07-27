import typing as t

from ..sanitizer.spec import (
    IdentitySanitizerSpec,
    LookupSanitizerItemSpec,
    LookupSanitizerSpec,
    MultiLookupSanitizerSpec,
    OmitSanitizerSpec,
    SanitizerSpec,
)

from ..common.table import (
    Omitted,
    Some,
    TableValue,
    Redacted,
)

from .model import (
    IdentitySanitizer,
    OmitSanitizer,
    RowSanitizer,
    LookupSanitizer,
)

def from_sanitizer_value(v: str | None):
    if v is None:
        return Redacted()

    if v == '':
        return Omitted()

    return Some(v)


def to_sanitizer_value(tv: TableValue[t.Any]):
    match tv:
        case Some(value=value) if isinstance(value, str):
            return value
        case Omitted():
            return ""
        case Redacted():
            return None
        case _:
            raise Exception("Error: cannot convert {} to sanitizer value".format(tv))

def ensure_tuple(v: None | str | t.Tuple[str]):
    if v is None:
        return (None,)
    if isinstance(v, str):
        return (v,)
    return v

def sanitizer_fromspec(name: str, spec: SanitizerSpec):
    match spec:
        case IdentitySanitizerSpec():
            return IdentitySanitizer(
                name=name,
                source=spec.source,
                remote_id=spec.remote_id,
                prompt=spec.prompt,
            )
        case LookupSanitizerSpec():
            return LookupSanitizer(
                name=name,
                sources={ spec.source: (spec.remote_id,) },
                remote_ids=(spec.remote_id,),
                prompt=spec.prompt,
                map={
                    tuple(from_sanitizer_value(i) for i in ensure_tuple(san.unsafe)):
                        tuple(from_sanitizer_value(i) for i in ensure_tuple(san.safe))
                    for san in spec.sanitizer
                },
            )
        case MultiLookupSanitizerSpec():
            return LookupSanitizer(
                name=name,
                sources=spec.sources,
                remote_ids=spec.new_ids,
                prompt=None,
                map={
                    tuple(from_sanitizer_value(i) for i in ensure_tuple(san.unsafe)):
                        tuple(from_sanitizer_value(i) for i in ensure_tuple(san.safe))
                    for san in spec.sanitizer
                },
            )
        case OmitSanitizerSpec():
            return OmitSanitizer(
                name=name,
                source=spec.source,
                remote_id=spec.remote_id,
                prompt=spec.prompt,
            )


def sanitizeritem_tospec(key: t.Tuple[TableValue[t.Any], ...], value: t.Tuple[TableValue[t.Any]]):
    k = tuple(to_sanitizer_value(i) for i in key)
    v = tuple(to_sanitizer_value(i) for i in value)
    return LookupSanitizerItemSpec(
        unsafe=k[0] if len(k) == 1 else k,
        safe=v[0] if len(v) == 1 else v,
    )

def sanitizer_tospec(sanitizer: RowSanitizer):
    match sanitizer:
        case LookupSanitizer():
            if sanitizer.prompt is None:
                return MultiLookupSanitizerSpec(
                    sources=sanitizer.sources,
                    new_ids=sanitizer.remote_ids,
                    action="sanitize",
                    sanitizer=tuple(sanitizeritem_tospec(k, v) for k, v in sanitizer.map.items()),
                )
            else:
                 return LookupSanitizerSpec(
                    source=list(sanitizer.sources)[0],
                    remote_id=sanitizer.remote_ids[0],
                    prompt=sanitizer.prompt,
                    action="sanitize",
                    sanitizer=tuple(sanitizeritem_tospec(k, v) for k, v in sanitizer.map.items()),
                )

        case IdentitySanitizer():
            return IdentitySanitizerSpec(
                source=sanitizer.source,
                remote_id=sanitizer.remote_id,
                prompt=sanitizer.prompt,
                action="bless",
            )
        case OmitSanitizer():
            return OmitSanitizerSpec(
                source=sanitizer.source,
                remote_id=sanitizer.remote_id,
                prompt=sanitizer.prompt,
                action="omit",
            )