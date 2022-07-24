import typing as t
import csv
import re
import hashlib
import io

from ..sanitizer.spec import (
    IdentitySanitizerSpec,
    OmitSanitizerSpec,
    SafeSanitizerItemSpec,
    SanitizerSpec,
    SimpleSanitizerSpec,
    StudySanitizerSpec,
    UnsafeSanitizerItemSpec,
)

from ..common.table import (
    Redacted,
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
    SanitizerUpdate,
    StudySanitizer,
    TableSanitizer,
)

def is_header_safe(header: str):
    return re.match(r'^\(.+\)$', header) is None

def rename_unsafe_header(header: str):
    return header[1: -1]

def to_csv_header(cid: SanitizedColumnId | UnsanitizedColumnId):
    match cid:
        case SanitizedColumnId():
            return cid.name
        case UnsanitizedColumnId():
            return "({})".format(cid.unsafe_name)

def hash_row(row: UnsanitizedTableRowView):
    values=",".join(tuple(c.unsafe_name+to_csv_value(v) for c, v in sorted(row.items(), key=lambda c: c[0].unsafe_name)))
    return hashlib.sha256(values.encode()).hexdigest()

def to_csv_value(tv: TableValue[t.Any]):
    match tv:
        case Some(value=value) if isinstance(value, str):
            return value
        case Omitted():
            return ""
        case _:
            raise Exception("Error: cannot convert {} to csv value".format(tv))
            
def write_sanitizer_update(f: io.TextIOBase, update: SanitizerUpdate, new: bool):
    writer = csv.writer(f)

    if new:
        writer.writerow((to_csv_header(cid) for cid in update.header))

    writer.writerows((
        (
            to_csv_value(row.get(cid)) if isinstance(cid, UnsanitizedColumnId) else ""
                for cid in update.header
        ) for row in update.rows
    ))

def sanitizer_fromspec(spec: SanitizerSpec):
    match spec:
        case IdentitySanitizerSpec():
            return IdentitySanitizer(
                name=spec.remote_id,
                key_col_ids=(UnsanitizedColumnId(spec.remote_id),),
                prompt=spec.prompt,
            )
        case SimpleSanitizerSpec():
            return LookupSanitizer(
                key_col_ids=(UnsanitizedColumnId(spec.remote_id),),
                new_col_ids=(SanitizedColumnId(spec.remote_id),),
                prompt=spec.prompt,
                map={
                    san.checksum: ((SanitizedColumnId(spec.remote_id), Some(san.safe) if isinstance(san, SafeSanitizerItemSpec) else Redacted(san.unsafe)),) for san in spec.sanitizer
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

def sanitizeritem_tospec(checksum: str, item: TableValue[t.Any]):
    match item:
        case Some(value=value) if isinstance(value, str):
            return SafeSanitizerItemSpec(
                checksum=checksum,
                safe=value,
            )
        case Redacted(value=value):
            return UnsafeSanitizerItemSpec(
                checksum=checksum,
                unsafe=value,
            )
        case _:
            raise Exception("Error: cannot convert {} to csv value".format(item))

def sanitizer_tospec(sanitizer: RowSanitizer):
    match sanitizer:
        case LookupSanitizer():
            return SimpleSanitizerSpec(
                remote_id=sanitizer.key_col_ids[0].unsafe_name,
                prompt=sanitizer.prompt,
                action="sanitize",
                sanitizer=tuple(
                    sanitizeritem_tospec(checksum, item) for checksum, ((_, item),) in sanitizer.map.items()
                )
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