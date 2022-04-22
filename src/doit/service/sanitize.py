import typing as t

from ..sanitizer.model import (
    Sanitizer,
)

from ..unsanitizedtable.model import (
    UnsanitizedTable,
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

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[Sanitizer]) -> SanitizedTable:
    
    # Step 1: Create columns for the sanitized table
    
    sanitized_columns = tuple(
        SanitizedColumnInfo(
            id=id,
            prompt="; ".join(c.prompt for c in table.schema.columns if c.id in sanitizer.key_col_ids),
            sanitizer_checksum=sanitizer.checksum,
        ) for sanitizer in sanitizers
            for id in sanitizer.new_col_ids
    )
    
    safe_col_ids = frozenset(c.id for c in table.schema.columns if c.is_safe)

    safe_columns = tuple(
        SanitizedColumnInfo(
            id=SanitizedColumnId(c.id.unsafe_name),
            prompt=c.prompt,
            sanitizer_checksum=None,
        ) for c in table.schema.columns if c.id in safe_col_ids
    )

    all_columns = sanitized_columns + safe_columns

    # Step 2: Create rowviews that map column names to sanitized/safe values

    sanitized_rows = (
        (sanitizer.get(row.subset(sanitizer.key_col_ids)) for sanitizer in sanitizers)
            for row in table.data.str_rows
    )

    safe_rows = (
        row.subset(safe_col_ids).bless_ids(lambda id: SanitizedColumnId(id.unsafe_name))
            for row in table.data.rows
    )

    all_rows = tuple(
        SanitizedTableRowView.combine_views(*sanitized, safe)
            for sanitized, safe in zip(sanitized_rows, safe_rows)
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
            rows=all_rows,
        ),
    )