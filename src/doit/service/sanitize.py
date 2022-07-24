import typing as t

from ..common.table import (
    LookupSanitizerMiss,
    Omitted,
    ErrorValue,
    Redacted,
)

from ..sanitizer.model import (
    OmitSanitizer,
    RowSanitizer,
    LookupSanitizer,
    IdentitySanitizer,
    StudySanitizer,
    TableSanitizer,
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

import re

from ..sanitizer.io import hash_row,to_csv_value

def update_lookupsanitizer(table: UnsanitizedTable, lookup_sanitizer: LookupSanitizer) -> LookupSanitizer:
    missing_rows = {
        hash_row(row): ((lookup_sanitizer.new_col_ids[0], Redacted(",".join(to_csv_value(row.get(UnsanitizedColumnId(safeid.unsafe_name))) for safeid in lookup_sanitizer.key_col_ids))),) for row in frozenset(table.data.subset(lookup_sanitizer.key_col_ids).rows)
            if hash_row(row) not in lookup_sanitizer.map and row.has_some()
    }
    return LookupSanitizer(
        key_col_ids=lookup_sanitizer.key_col_ids,
        new_col_ids=lookup_sanitizer.new_col_ids,
        prompt=lookup_sanitizer.prompt,
        map={**lookup_sanitizer.map, **missing_rows},
    )

def update_tablesanitizer(table: UnsanitizedTable, table_sanitizer: TableSanitizer):
    missing_columns = tuple(
        (
            IdentitySanitizer(
                name=c.id.unsafe_name,
                prompt=re.sub(r'\s+', ' ', c.prompt),
                key_col_ids=(UnsanitizedColumnId(c.id.unsafe_name),),
            ) if c.is_safe else
            LookupSanitizer(
                key_col_ids=(c.id,),
                new_col_ids=(SanitizedColumnId(c.id.unsafe_name),),
                prompt=re.sub(r'\s+', ' ', c.prompt),
                map={},
            )
        ) for c in table.schema if c.id.unsafe_name not in table_sanitizer.sanitizers
    )

    return TableSanitizer(
        table_name=table_sanitizer.table_name,
        sanitizers=tuple(
            update_lookupsanitizer(table, s) if (isinstance(s, LookupSanitizer)) else s
                for s in (table_sanitizer.sanitizers + missing_columns)
        )
    )

def update_studysanitizers(table_name: str, table: UnsanitizedTable, study_sanitizer: StudySanitizer):
    missing_tables = {
        table_name: TableSanitizer(
            table_name=table_name,
            sanitizers=()
        )
    } if table_name not in study_sanitizer.table_sanitizers else {}

    return StudySanitizer(
        table_sanitizers={
            name: (update_tablesanitizer(table, san) if table_name == name else san)
                for name, san in {**study_sanitizer.table_sanitizers, **missing_tables}.items()
        }
    )

def sanitize_row(row: UnsanitizedTableRowView, sanitizer: RowSanitizer):
    row_subset = row.subset(sanitizer.key_col_ids) # TODO: Only subset in the case of LookupSanitizer

    # If any row keys are Error, return that Error for all new vals
    error = next((v for v in row_subset.values() if isinstance(v, ErrorValue)), None)
    if error:
        return tuple((k, error) for k in sanitizer.new_col_ids)

    match sanitizer:
        case LookupSanitizer():
            # If all row keys are Missing, return Missing for all new vals
            if (all(isinstance(v, Omitted) for v in row_subset.values())):
                return tuple((k, Omitted()) for k in sanitizer.new_col_ids)

            # Lookup the new sanitized columns using the key columns
            return sanitizer.map.get(hash_row(row_subset)) or tuple(
                (k,ErrorValue(LookupSanitizerMiss(row_subset, sanitizer.map)))
                    for k in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(
                (new, row_subset.get(old))
                    for old, new in zip(sanitizer.key_col_ids, sanitizer.new_col_ids)
            )
        case OmitSanitizer():
            return ()

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

def sanitize_columns(column_lookup: t.Mapping[UnsanitizedColumnId, UnsanitizedColumnInfo], sanitizer: RowSanitizer) -> t.Tuple[SanitizedColumnInfo, ...]:
    match sanitizer:
        case LookupSanitizer():
            return tuple(
                SanitizedSimpleColumnInfo(
                    id=id,
                    prompt="; ".join(column_lookup[c].prompt for c in sanitizer.key_col_ids),
                    sanitizer_checksum="",
                    sortkey="_".join(column_lookup[c].sortkey for c in sanitizer.key_col_ids)
                ) for id in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(bless_column_info(column_lookup[id]) for id in sanitizer.key_col_ids)
        case OmitSanitizer():
            return ()

def sanitize_table(table: UnsanitizedTable, table_sanitizer: TableSanitizer) -> SanitizedTable:

    # TODO: Guard against users making new columns with duplicate names
    
    # Sanitize columns and rows

    column_info_lookup = { c.id: c for c in table.schema }

    sanitized_columns_unsorted = tuple(
        c
            for sanitizer in table_sanitizer.sanitizers
                for c in sanitize_columns(column_info_lookup, sanitizer)
    )

    sanitized_columns = sorted(sanitized_columns_unsorted, key=lambda x: x.sortkey)

    sanitized_rows = tuple(
        SanitizedTableRowView(
            v
                for sanitizer in table_sanitizer.sanitizers
                    for v in sanitize_row(row, sanitizer) 
        )
            for row in table.data.rows
    )
    
    # And then you're done!

    return SanitizedTable(
        info=SanitizedTableInfo(
            name=table_sanitizer.table_name,
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