import typing as t

from ..common import (
    TableRowView,
    Omitted,
    Error,
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

    # If all row keys are Missing, return Missing for all new vals
    if (all(isinstance(v, Omitted) for v in row_subset.values())):
        return TableRowView({ k: Omitted() for k in sanitizer.new_col_ids })

    # If any row keys are Error, return that Error for all new vals
    error = next((v for v in row_subset.values() if isinstance(v, Error)), None)
    if error:
        return TableRowView({ k: error for k in sanitizer.new_col_ids })

    match sanitizer:
        case LookupSanitizer():
            if row_subset.has_value_type(str):
                return sanitizer.map.get(row_subset) or TableRowView(
                    { k: Error('missing_sanitizer', row_subset) for k in sanitizer.new_col_ids }
                )
        case IdentitySanitizer():
            if row_subset.has_value_type(str):
                return TableRowView(
                    { new: row_subset.get(old) for old, new in zip(sanitizer.key_col_ids, sanitizer.new_col_ids) } 
                )

    return TableRowView(
        { k: Error('sanitizer_type_mismatch', (row_subset, sanitizer)) for k in sanitizer.new_col_ids }
    )

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[LookupSanitizer]) -> SanitizedTable:

    # Step 0: Create special sanitizers

    safe_column_sanitizer = IdentitySanitizer(
        key_col_ids=tuple(c.id for c in table.schema if c.is_safe)
    )
    
    # Step 1: Create columns for the sanitized table
    
    sanitized_columns = tuple(
        SanitizedColumnInfo(
            id=id,
            prompt="; ".join(c.prompt for c in table.schema if c.id in sanitizer.key_col_ids),
            sanitizer_checksum=sanitizer.checksum,
        ) for sanitizer in sanitizers
            for id in sanitizer.new_col_ids
    )
    
    safe_columns = tuple(
        SanitizedColumnInfo(
            id=SanitizedColumnId(c.id.unsafe_name),
            prompt=c.prompt,
            sanitizer_checksum=None,
        ) for c in table.schema if c.id in safe_column_sanitizer.key_col_ids
    )

    # Step 2: Combine sanitizers / columns, ensuring they're stacked in the same order

    all_sanitizers = (*sanitizers, safe_column_sanitizer)
    all_columns = sanitized_columns + safe_columns

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