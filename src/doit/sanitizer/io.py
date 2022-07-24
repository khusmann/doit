import typing as t
import csv
import re
import hashlib
import io

from ..sanitizer.spec import (
    IdentitySanitizerSpec,
    MultilineSimpleSanitizerSpec,
    OmitSanitizerSpec,
    SanitizerItemSpec,
    SanitizerSpec,
    SimpleSanitizerSpec,
#    SimpleSanitizerSpec,
    StudySanitizerSpec,
    TableLookupSanitizerSpec,
    TableSanitizerSpec
)

from ..common.table import (
    Omitted,
    Redacted,
    Some,
    DuplicateHeaderError,
    EmptyHeaderError,
    EmptySanitizerKeyError,
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

def write_sanitizer_csv(sanitizer: LookupSanitizer):
    import io
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow((to_csv_header(cid) for cid in sanitizer.header))

    writer.writerows((
        (
            to_csv_value(unsafe.get(cid)) if isinstance(cid, UnsanitizedColumnId) else dict(safe).get(cid)
                for unsafe, safe in sanitizer.map.items()
        ) for cid in sanitizer.header
    ))

    return buffer.getvalue()

def load_sanitizer_csv(csv_text: str, name: str) -> LookupSanitizer:
    reader = csv.reader(io.StringIO(csv_text, newline=''))

    header_str = tuple(next(reader))

    lines = tuple(reader)

    if not all(header_str):
        raise EmptyHeaderError(header_str)

    if len(set(header_str)) != len(header_str):
        raise DuplicateHeaderError(header_str)

    header = tuple(
        SanitizedColumnId(c) if is_header_safe(c) else UnsanitizedColumnId(rename_unsafe_header(c))
            for c in header_str
    )

    keys = tuple(
        UnsanitizedTableRowView(
            (c, Some(v) if v else Omitted())
                for c, v in zip(header, row) if isinstance(c, UnsanitizedColumnId)
        ) for row in lines
    )

    values = tuple(
        tuple(
            (c, Some(v) if v else Redacted())
                for c, v in zip(header, row)if isinstance(c, SanitizedColumnId)
        ) for row in lines
    )


    for key, value in zip(keys, values):
        # Insure key columns have at least one real value
        if not any(isinstance(k, Some) for k in key.values()):
            raise EmptySanitizerKeyError(value)

    return LookupSanitizer(
        name=name,
        prompt="",
        map=dict(zip(keys, values)),
        header=header,
        checksum=hashlib.sha256(csv_text.encode()).hexdigest(),
    )

def sanitizer_fromspec(name: str, spec: SanitizerSpec):
    match spec:
        case TableLookupSanitizerSpec():
            return load_sanitizer_csv(spec.sanitizer, name)
        case IdentitySanitizerSpec():
            return IdentitySanitizer(
                name=name,
                key_col_ids=(UnsanitizedColumnId(name),),
                prompt=spec.prompt,
            )
        case SimpleSanitizerSpec():
            return LookupSanitizer(
                name=name,
                prompt=spec.prompt,
                map={
                    UnsanitizedTableRowView(
                        ((UnsanitizedColumnId(name), Some(unsafe)),)
                    ): ((SanitizedColumnId(name), Some(safe)),) for unsafe, safe in spec.sanitizer.items()
                },
                header=(UnsanitizedColumnId(name), SanitizedColumnId(name)),
                checksum="",
            )
        case OmitSanitizerSpec():
            return OmitSanitizer(
                name=name,
                prompt=spec.prompt,
            )
        case MultilineSimpleSanitizerSpec():
            return LookupSanitizer(
                name=name,
                prompt=spec.prompt,
                map={
                    UnsanitizedTableRowView(
                        ((UnsanitizedColumnId(name), Some(item.unsafe)),)
                    ): ((SanitizedColumnId(name), Some(item.safe)),) for item in spec.sanitizer
                },
                header=(UnsanitizedColumnId(name), SanitizedColumnId(name)),
                checksum="",
            )


def tablesanitizer_fromspec(table_name: str, spec: TableSanitizerSpec):
    return TableSanitizer(
        table_name=table_name,
        sanitizers={ name: sanitizer_fromspec(name, san) for name, san in spec.items() },
    )

def studysanitizer_fromspec(spec: StudySanitizerSpec):
    return StudySanitizer(
        table_sanitizers={ name: tablesanitizer_fromspec(name, san) for name, san in spec.items() }
    )

def sanitizer_tospec(sanitizer: RowSanitizer):
    match sanitizer:
        case LookupSanitizer():
            if len(sanitizer.key_col_ids) == 1 and len(sanitizer.new_col_ids) == 1:
                if all(len(to_csv_value(unsafe.get(UnsanitizedColumnId(sanitizer.name)))) < 20 for unsafe in sanitizer.map):
                    return SimpleSanitizerSpec(
                        prompt=sanitizer.prompt,
                        action="sanitize",
                        sanitizer={
                            to_csv_value(unsafe.get(UnsanitizedColumnId(sanitizer.name))): to_csv_value(safe)
                                for unsafe, ((_, safe),) in sanitizer.map.items()
                        }
                    )
                else:
                    return MultilineSimpleSanitizerSpec(
                        prompt=sanitizer.prompt,
                        action="sanitize",
                        sanitizer=tuple(
                            SanitizerItemSpec(
                                unsafe=to_csv_value(unsafe.get(UnsanitizedColumnId(sanitizer.name))),
                                safe=to_csv_value(safe)
                            )
                                for unsafe, ((_, safe),) in sanitizer.map.items()
                        )
                    )
            else:
                return TableLookupSanitizerSpec(
                    prompt=sanitizer.prompt,
                    action="sanitize",
                    sanitizer=write_sanitizer_csv(sanitizer),
                )
        case IdentitySanitizer():
            return IdentitySanitizerSpec(
                prompt=sanitizer.prompt,
                action="bless",
            )
        case OmitSanitizer():
            return OmitSanitizerSpec(
                prompt=sanitizer.prompt,
                action="omit",
            )

def studysanitizer_tospec(study_sanitizer: StudySanitizer):
    return {
        t.table_name: {
                s.name: sanitizer_tospec(s).dict() for s in t.sanitizers.values()
        } for t in sorted(study_sanitizer.table_sanitizers.values(), key=lambda i: i.table_name)
    }