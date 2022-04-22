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

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[LookupSanitizer]) -> SanitizedTable:

    # Step 1: Create identity sanitizer for safe columns

    safe_column_sanitizer = IdentitySanitizer(
        key_col_ids=tuple(c.id for c in table.schema if c.is_safe)
    )
    
    all_sanitizers = (*sanitizers, safe_column_sanitizer)
    
    # Step 2: Create columns for the sanitized table

    def columns_from_sanitizer(sanitizer: Sanitizer):
        match sanitizer:
            case LookupSanitizer():
                return (
                    SanitizedColumnInfo(
                        id=id,
                        prompt="; ".join(c.prompt for c in table.schema if c.id in sanitizer.key_col_ids),
                        sanitizer_checksum=sanitizer.checksum,
                    ) for id in sanitizer.new_col_ids
                )
            case IdentitySanitizer():
                return (
                    SanitizedColumnInfo(
                        id=SanitizedColumnId(c.id.unsafe_name),
                        prompt=c.prompt,
                        sanitizer_checksum=None,
                    ) for c in table.schema if c.id in safe_column_sanitizer.key_col_ids
                )

    all_columns = tuple(
        c
            for sanitizer in all_sanitizers
                for c in columns_from_sanitizer(sanitizer)
    )
    
    # Step 3: And then you're done!

    return SanitizedTable(
        info=SanitizedTableInfo(
            data_checksum=table.data_checksum,
            schema_checksum=table.schema_checksum,
            columns=all_columns,
        ),
        data=SanitizedTableData(
            column_ids=tuple(c.id for c in all_columns),
            rows=tuple(
                SanitizedTableRowView.combine_views(
                    *(sanitize_row(row, sanitizer) for sanitizer in all_sanitizers)
                ) for row in table.data.rows
            ),
        ),
    )