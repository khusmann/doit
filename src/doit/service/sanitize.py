import typing as t

from ..common.table import (
    LookupSanitizerMiss,
    Omitted,
    ErrorValue,
)

from ..sanitizer.model import (
    Sanitizer,
    LookupSanitizer,
    IdentitySanitizer,
    SanitizerUpdate,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedOrdinalColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableRowView,
    UnsanitizedTextColumnInfo,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedTable,
    SanitizedTableInfo,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedTableRowView,
    SanitizedTextColumnInfo,
    SanitizedOrdinalColumnInfo,
)

def update_tablesanitizers(table: UnsanitizedTable, sanitizers: t.Mapping[str, LookupSanitizer]):
    unsafe_columns = frozenset(c.id for c in table.schema if not c.is_safe)
    sanitized_columns = frozenset(
        c
            for s in sanitizers.values()
                for c in s.key_col_ids
    )

    missing_columns = unsafe_columns - sanitized_columns

    missing_columns_updates = {
        c.unsafe_name: SanitizerUpdate(
            new=True,
            header=(c, SanitizedColumnId(c.unsafe_name)),
            rows=tuple({ row for row in table.data.subset([c]).rows if row.has_some() }),
        ) for c in missing_columns
    }

    missing_rows_updates = {
        name: SanitizerUpdate(
            new=False,
            header=sanitizer.header,
            rows=tuple(
                row for row in frozenset(table.data.subset(sanitizer.key_col_ids).rows)
                    if row not in sanitizer.map and row.has_some()
            )
        ) for name, sanitizer in sanitizers.items()
    }

    return missing_columns_updates | missing_rows_updates


def sanitize_row(row: UnsanitizedTableRowView, sanitizer: Sanitizer):
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
            return sanitizer.map.get(row_subset) or tuple(
                (k,ErrorValue(LookupSanitizerMiss(row_subset, sanitizer.map)))
                    for k in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(
                (new, row_subset.get(old))
                    for old, new in zip(sanitizer.key_col_ids, sanitizer.new_col_ids)
            )

def bless_column_info(column_info: UnsanitizedColumnInfo) -> SanitizedColumnInfo:
    id = SanitizedColumnId(column_info.id.unsafe_name)
    match column_info:
        case UnsanitizedTextColumnInfo():
            return SanitizedTextColumnInfo(
                id=id,
                prompt=column_info.prompt,
                sanitizer_checksum=None,
            )
        case UnsanitizedOrdinalColumnInfo():
            return SanitizedOrdinalColumnInfo(
                id=id,
                prompt=column_info.prompt,
                codes=column_info.codes,
                value_type=column_info.value_type,
            )

def sanitize_columns(column_lookup: t.Mapping[UnsanitizedColumnId, UnsanitizedColumnInfo], sanitizer: Sanitizer) -> t.Tuple[SanitizedColumnInfo, ...]:
    match sanitizer:
        case LookupSanitizer():
            return tuple(
                SanitizedTextColumnInfo(
                    id=id,
                    prompt="; ".join(column_lookup[c].prompt for c in sanitizer.key_col_ids),
                    sanitizer_checksum=sanitizer.checksum,
                ) for id in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(bless_column_info(column_lookup[id]) for id in sanitizer.key_col_ids)

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[LookupSanitizer]) -> SanitizedTable:

    # Step 1: Create identity sanitizer for safe columns

    safe_column_sanitizer = IdentitySanitizer(
        key_col_ids=tuple(c.id for c in table.schema if c.is_safe)
    )
    
    all_sanitizers = (*sanitizers, safe_column_sanitizer)
    
    # Step 2: Sanitize columns and rows

    column_info_lookup = { c.id: c for c in table.schema }

    sanitized_columns = tuple(
        c
            for sanitizer in all_sanitizers
                for c in sanitize_columns(column_info_lookup, sanitizer)
    )

    sanitized_rows = tuple(
        SanitizedTableRowView(
            v
                for sanitizer in all_sanitizers
                    for v in sanitize_row(row, sanitizer) 
        )
            for row in table.data.rows
    )
    
    # Step 3: And then you're done!

    return SanitizedTable(
        info=SanitizedTableInfo(
            data_checksum=table.data_checksum,
            schema_checksum=table.schema_checksum,
            columns=sanitized_columns,
        ),
        data=SanitizedTableData(
            column_ids=tuple(c.id for c in sanitized_columns),
            rows=sanitized_rows,
        ),
    )