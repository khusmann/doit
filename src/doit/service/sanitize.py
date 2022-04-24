import typing as t

from ..common import (
    LookupSanitizerMiss,
    TableRowView,
    Omitted,
    ErrorValue,
)

from ..sanitizer.model import (
    Sanitizer,
    LookupSanitizer,
    IdentitySanitizer,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedColumnInfo,
    UnsanitizedMultiselectColumnInfo,
    UnsanitizedTable,
    UnsanitizedTableRowView,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedTable,
    SanitizedTableInfo,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedTableRowView,
)

#def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsanitizedTable):
#    subset_rows = (row.subset(sanitizer.key_col_ids) for row in table.iter_rows())
#    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

def sanitize_row(row: UnsanitizedTableRowView, sanitizer: Sanitizer) -> SanitizedTableRowView:
    row_subset = row.subset(sanitizer.key_col_ids)

    # If any row keys are Error, return that Error for all new vals
    error = next((v for v in row_subset.values() if isinstance(v, ErrorValue)), None)
    if error:
        return TableRowView({ k: error for k in sanitizer.new_col_ids })

    match sanitizer:
        case LookupSanitizer():
            # If all row keys are Missing, return Missing for all new vals
            if (all(isinstance(v, Omitted) for v in row_subset.values())):
                return TableRowView({ k: Omitted() for k in sanitizer.new_col_ids })

            # Lookup the new sanitized columns using the key columns
            return sanitizer.map.get(row_subset) or TableRowView(
                { k: ErrorValue(LookupSanitizerMiss(row_subset, sanitizer.map)) for k in sanitizer.new_col_ids }
            )
        case IdentitySanitizer():
            return TableRowView(
                { new: row_subset.get(old) for old, new in zip(sanitizer.key_col_ids, sanitizer.new_col_ids) } 
            )

def sanitize_columns(column_lookup: t.Mapping[UnsanitizedColumnId, UnsanitizedColumnInfo], sanitizer: Sanitizer) -> t.Tuple[SanitizedColumnInfo, ...]:
    match sanitizer:
        case LookupSanitizer():
            return tuple(
                SanitizedColumnInfo(
                    id=id,
                    prompt="; ".join(column_lookup[c].prompt for c in sanitizer.key_col_ids),
                    sanitizer_checksum=sanitizer.checksum,
                    type='text',
                ) for id in sanitizer.new_col_ids
            )
        case IdentitySanitizer():
            return tuple(
                SanitizedColumnInfo(
                    id=SanitizedColumnId(column_lookup[id].id.unsafe_name),
                    prompt=column_lookup[id].prompt,
                    sanitizer_checksum=None,
                    type='multiselect' if isinstance(id, UnsanitizedMultiselectColumnInfo) else 'text',
                ) for id in sanitizer.key_col_ids
            )

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
        SanitizedTableRowView.combine_views(
            *(sanitize_row(row, sanitizer) for sanitizer in all_sanitizers)
        ) for row in table.data.rows
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