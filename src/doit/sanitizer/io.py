import typing as t
import hashlib

from ..sanitizer.spec import (
    IdentitySanitizerSpec,
    MultiSanitizerSpec,
    OmitSanitizerSpec,
    SanitizerSpec,
    StudySanitizerSpec,
)

from ..common.table import (
    Omitted,
    Some,
    TableValue,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTableRowView,
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

def hash_row(row: UnsanitizedTableRowView):
    values=",".join(tuple(c.unsafe_name+to_sanitizer_value(v) for c, v in sorted(row.items(), key=lambda c: c[0].unsafe_name)))
    return hashlib.sha256(values.encode()).hexdigest()

def to_sanitizer_value(tv: TableValue[t.Any]):
    match tv:
        case Some(value=value) if isinstance(value, str):
            return value
        case Omitted():
            return ""
        case _:
            raise Exception("Error: cannot convert {} to sanitizer value".format(tv))
            
def sanitizer_fromspec(spec: SanitizerSpec):
    match spec:
        case IdentitySanitizerSpec():
            return IdentitySanitizer(
                name=spec.remote_id,
                key_col_ids=(UnsanitizedColumnId(spec.remote_id),),
                prompt=spec.prompt,
            )
        case MultiSanitizerSpec():
            return LookupSanitizer(
                key_col_ids=tuple(UnsanitizedColumnId(i) for i in spec.src_remote_ids),
                new_col_ids=tuple(SanitizedColumnId(i) for i in spec.dst_remote_ids),
                prompt=spec.prompt,
                map=spec.sanitizer,
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

def sanitizer_tospec(sanitizer: RowSanitizer):
    match sanitizer:
        case LookupSanitizer():
            return MultiSanitizerSpec(
                src_remote_ids=tuple(i.unsafe_name for i in sanitizer.key_col_ids),
                dst_remote_ids=tuple(i.name for i in sanitizer.new_col_ids),
                prompt=sanitizer.prompt,
                action="sanitize",
                sanitizer=sanitizer.map,
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