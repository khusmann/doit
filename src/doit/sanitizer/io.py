import typing as t

from ..sanitizer.spec import (
    IdentitySanitizerSpec,
    LookupSanitizerItemSpec,
    LookupSanitizerSpec,
    OmitSanitizerSpec,
    SanitizerSpec,
    StudySanitizerSpec,
)

from ..common.table import (
    Omitted,
    Some,
    TableValue,
    Redacted,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
)

from .model import (
    IdentitySanitizer,
    OmitSanitizer,
    RowSanitizer,
    SanitizedColumnId,
    LookupSanitizer,
    StudySanitizer,
    TableSanitizer,
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

def sanitizer_fromspec(spec: SanitizerSpec):
    match spec:
        case IdentitySanitizerSpec():
            return IdentitySanitizer(
                name=spec.remote_id,
                key_col_ids=(UnsanitizedColumnId(spec.remote_id),),
                prompt=spec.prompt,
            )
        case LookupSanitizerSpec():
            return LookupSanitizer(
                key_col_ids=tuple(UnsanitizedColumnId(i) for i in spec.src_remote_ids),
                new_col_ids=tuple(SanitizedColumnId(i) for i in spec.dst_remote_ids),
                prompt=spec.prompt,
                map={
                    tuple(from_sanitizer_value(i) for i in ensure_tuple(san.unsafe)):
                        tuple(from_sanitizer_value(i) for i in ensure_tuple(san.unsafe))
                    for san in spec.sanitizer
                },
            )
        case OmitSanitizerSpec():
            return OmitSanitizer(
                name=spec.remote_id,
                prompt=spec.prompt,
            )


def studysanitizer_fromspec(spec: StudySanitizerSpec):
    return StudySanitizer(
        table_sanitizers={
            name: TableSanitizer(
                table_name=name,
                sanitizers=tuple(sanitizer_fromspec(i) for i in sans),
            ) for name, sans in spec.items()
        }
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
            return LookupSanitizerSpec(
                src_remote_ids=tuple(i.unsafe_name for i in sanitizer.key_col_ids),
                dst_remote_ids=tuple(i.name for i in sanitizer.new_col_ids),
                prompt=sanitizer.prompt,
                action="sanitize",
                sanitizer=tuple(sanitizeritem_tospec(k, v) for k, v in sanitizer.map.items()),
            )
        case IdentitySanitizer():
            return IdentitySanitizerSpec(
                remote_id=sanitizer.name,
                prompt=sanitizer.prompt,
                action="bless",
            )
        case OmitSanitizer():
            return OmitSanitizerSpec(
                remote_id=sanitizer.name,
                prompt=sanitizer.prompt,
                action="omit",
            )

def studysanitizer_tospec(study_sanitizer: StudySanitizer):
    return {
        t.table_name: [
                sanitizer_tospec(s).dict() for s in t.sanitizers
        ] for t in sorted(study_sanitizer.table_sanitizers.values(), key=lambda i: i.table_name)
    }