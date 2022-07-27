import typing as t

from ..sanitizer.io import to_sanitizer_value

from ..common.table import (
    LookupSanitizerMiss,
    Omitted,
    ErrorValue,
    Redacted,
    Some,
)

from ..sanitizer.model import (
    OmitSanitizer,
    RowSanitizer,
    LookupSanitizer,
    IdentitySanitizer,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedCodedColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableRowView,
    UnsanitizedSimpleColumnInfo,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedTable,
    SanitizedTableInfo,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedTableRowView,
    SanitizedSimpleColumnInfo,
    SanitizedCodedColumnInfo,
)

from ..sanitizer.io import to_sanitizer_value

import re

def hash_row(row: UnsanitizedTableRowView, key_col_ids: t.Tuple[UnsanitizedColumnId, ...]):
    return tuple(row.get(c) for c in key_col_ids)

def sort_sanitizer_map(sanitizer: RowSanitizer) -> RowSanitizer:
    if not isinstance(sanitizer, LookupSanitizer):
        return sanitizer

    return LookupSanitizer(
        name=sanitizer.name,
        sources=sanitizer.sources,
        remote_ids=sanitizer.remote_ids,
        prompt=sanitizer.prompt,
        map=dict(sorted(sanitizer.map.items(), key=lambda k: tuple(to_sanitizer_value(i) for i in k[0])))
    )

def update_sanitizer(table: UnsanitizedTable, sanitizer: RowSanitizer) -> RowSanitizer:
    if not isinstance(sanitizer, LookupSanitizer):
        return sanitizer

    if not sanitizer.key_col_ids(table.name):
        return sanitizer

    missing_rows = {
        hash_row(row, sanitizer.key_col_ids((table.name))): tuple(Redacted() for _ in sanitizer.new_col_ids)
        for row in frozenset(table.data.subset(sanitizer.key_col_ids(table.name)).rows)
            if hash_row(row, sanitizer.key_col_ids(table.name)) not in sanitizer.map and row.has_some()
    }
    missing_rows_sorted = dict(sorted(missing_rows.items(), key=lambda i: tuple(to_sanitizer_value(j) for j in i[0])))
    return LookupSanitizer(
        name=sanitizer.name,
        sources=sanitizer.sources,
        remote_ids=sanitizer.remote_ids,
        prompt=sanitizer.prompt,
        map={**sanitizer.map, **missing_rows_sorted},
    )

def update_studysanitizers(table: UnsanitizedTable, existing_sanitizers: t.Tuple[RowSanitizer, ...]):
    existing_columns = {
        c 
            for s in existing_sanitizers if len(s.key_col_ids(table.name)) == 1
                for c in s.key_col_ids(table.name)
    }

    new_sanitizers = tuple(
        (
            IdentitySanitizer(
                name=table.name,
                source=table.name,
                prompt=re.sub(r'\s+', ' ', c.prompt),
                remote_id=c.id.unsafe_name,
            ) if c.is_safe else
            LookupSanitizer(
                name=table.name,
                sources={table.name: tuple(c.id.unsafe_name)},
                remote_ids=tuple(c.id.unsafe_name),
                prompt=re.sub(r'\s+', ' ', c.prompt),
                map={},
            )
        ) for c in table.schema if c.id not in existing_columns
    )

    return tuple(update_sanitizer(table, i) for i in (existing_sanitizers+new_sanitizers))
    
def lookup_totableval(v: str | None):
    if v is None:
        return Redacted()
    
    if v == "":
        return Omitted()

    return Some(v)

def sanitize_row(row: UnsanitizedTableRowView, sanitizer: RowSanitizer, table_name: str):
    row_subset = row.subset(sanitizer.key_col_ids(table_name)) # TODO: Only subset in the case of LookupSanitizer

    # If any row keys are Error, return that Error for all new vals
    error = next((v for v in row_subset.values() if isinstance(v, ErrorValue)), None)
    if error:
        return tuple((k, error) for k in sanitizer.new_col_ids)

    match sanitizer:
        case LookupSanitizer():
            # If all row keys are Missing, return Missing for all new vals
            if (all(isinstance(v, Omitted) for v in row_subset.values())):
                return tuple((k, Omitted()) for k in sanitizer.new_col_ids)

            new_values = sanitizer.map.get(hash_row(row_subset, sanitizer.key_col_ids(table_name)))

            if new_values is None:
                return tuple(
                    (k,ErrorValue(LookupSanitizerMiss(row_subset, sanitizer.map))) for k in sanitizer.new_col_ids
            )

            # Lookup the new sanitized columns using the key columns
            return tuple(zip(sanitizer.new_col_ids, new_values))
        case IdentitySanitizer():
            return tuple(
                (new, row_subset.get(old))
                    for old, new in zip(sanitizer.key_col_ids(table_name), sanitizer.new_col_ids)
            )
        case OmitSanitizer():
            return tuple(
                (new, Redacted()) for new in sanitizer.new_col_ids
            )

def bless_column_info(column_info: UnsanitizedColumnInfo) -> SanitizedColumnInfo:
    id = SanitizedColumnId(column_info.id.unsafe_name)
    match column_info:
        case UnsanitizedSimpleColumnInfo():
            return SanitizedSimpleColumnInfo(
                id=id,
                prompt=column_info.prompt,
                sanitizer_checksum=None,
                value_type=column_info.value_type,
                sortkey=column_info.sortkey,
            )
        case UnsanitizedCodedColumnInfo():
            return SanitizedCodedColumnInfo(
                id=id,
                prompt=column_info.prompt,
                codes=column_info.codes,
                value_type=column_info.value_type,
                sortkey=column_info.sortkey,
            )

def sanitize_columns(column_lookup: t.Mapping[UnsanitizedColumnId, UnsanitizedColumnInfo], sanitizer: RowSanitizer, table_name: str) -> t.Tuple[SanitizedColumnInfo, ...]:
    match sanitizer:
        case LookupSanitizer():
            return tuple(
                SanitizedSimpleColumnInfo(
                    id=id,
                    prompt="; ".join(column_lookup[c].prompt for c in sanitizer.key_col_ids(table_name)),
                    sanitizer_checksum="",
                    sortkey="_".join(column_lookup[c].sortkey for c in sanitizer.key_col_ids(table_name))
                ) for id in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(bless_column_info(column_lookup[id]) for id in sanitizer.key_col_ids(table_name))
        case OmitSanitizer():
            return tuple(bless_column_info(column_lookup[id]) for id in sanitizer.key_col_ids(table_name))

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Tuple[RowSanitizer, ...]) -> SanitizedTable:
    # TODO: Guard against users making new columns with duplicate names

    # Create identity sanitizers for safe columns if not already being sanitized

    used_sanitizers = tuple(i for i in sanitizers if i.key_col_ids(table.name))

    used_sanitizer_output = {
        UnsanitizedColumnId(c.name)
            for san in used_sanitizers
                for c in san.new_col_ids
    }

    identity_sanitizers = tuple(
        IdentitySanitizer(
            name=table.name,
            source=table.name,
            prompt=c.prompt,
            remote_id=c.id.unsafe_name,
        ) if c.is_safe else
        OmitSanitizer(
            name=table.name,
            source=table.name,
            prompt=c.prompt,
            remote_id=c.id.unsafe_name,
        ) for c in table.schema if c.id not in used_sanitizer_output
    )

    all_sanitizers = used_sanitizers + identity_sanitizers
    
    # Sanitize columns and rows

    column_info_lookup = { c.id: c for c in table.schema }

    sanitized_columns_unsorted = tuple(
        c
            for sanitizer in all_sanitizers
                for c in sanitize_columns(column_info_lookup, sanitizer, table.name)
    )

    sanitized_columns = sorted(sanitized_columns_unsorted, key=lambda x: x.sortkey)

    sanitized_rows = tuple(
        SanitizedTableRowView(
            v
                for sanitizer in all_sanitizers
                    for v in sanitize_row(row, sanitizer, table.name) 
        )
            for row in table.data.rows
    )
    
    # And then you're done!

    return SanitizedTable(
        info=SanitizedTableInfo(
            name=table.name,
            title=table.source_title,
            data_checksum=table.data_checksum,
            schema_checksum=table.schema_checksum,
            source=table.source_name,
            columns=tuple(sanitized_columns),
        ),
        data=SanitizedTableData(
            column_ids=tuple(c.id for c in sanitized_columns),
            rows=sanitized_rows,
        ),
    )