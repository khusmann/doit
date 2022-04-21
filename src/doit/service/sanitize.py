import typing as t
import re

from itertools import zip_longest

from ..common import (
    Some,
    Missing,
    TableRowView,
)

from ..sanitizer.model import (
    Sanitizer,
    SanitizerSpec,
)

from ..unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTable,
    UnsanitizedStrTableRowView,
)

from ..sanitizedtable.model import (
    SanitizedColumnId,
    SanitizedTable,
    SanitizedTableInfo,
    SanitizedColumnInfo,
    SanitizedTableData,
    SanitizedStrTableRowView,
)

#def missing_sanitizer_rows(sanitizer: Sanitizer, table: UnsanitizedTable):
#    subset_rows = (row.subset(sanitizer.key_col_ids) for row in table.iter_rows())
#    return tuple(subset_row for subset_row in subset_rows if subset_row.hash() not in sanitizer.map)

def sanitizer_from_spec(sanitizer_spec: SanitizerSpec) -> Sanitizer:
    key_col_names = {c: UnsanitizedColumnId(c[1:-1]) for c in sanitizer_spec.header if re.match(r'^\(.+\)$', c)}
    new_col_names = {c: SanitizedColumnId(c) for c in sanitizer_spec.header if c not in key_col_names}

    keys = (
        UnsanitizedStrTableRowView({
            key_col_names[c]: Some(v) if v else Missing('omitted')
                for c, v in zip_longest(sanitizer_spec.header, row) if c in key_col_names
        }) for row in sanitizer_spec.rows
    )

    values = (
        SanitizedStrTableRowView({
            new_col_names[c]: Some(v) if v else Missing('redacted')
                for c, v in zip_longest(sanitizer_spec.header, row)if c in new_col_names
        }) for row in sanitizer_spec.rows
    )

    hash_map = {
        key.hash_or_die(): new
            for key, new in zip(keys, values)
                if any(v for v in key.values()) # TODO: test sanitizers with blank key columns
    }

    return Sanitizer(
        key_col_ids=tuple(key_col_names.values()),
        new_col_ids=tuple(new_col_names.values()),
        map=hash_map,
        checksum=sanitizer_spec.checksum,
    )

def sanitize_table(table: UnsanitizedTable, sanitizers: t.Sequence[Sanitizer]) -> SanitizedTable:
    
    # Step 1: Create columns for the sanitized table
    
    sanitized_columns = tuple(
        SanitizedColumnInfo(
            id=id,
            prompt="; ".join(c.prompt for c in table.info.columns if c.id in sanitizer.key_col_ids),
            sanitizer_checksum=sanitizer.checksum,
        ) for sanitizer in sanitizers
            for id in sanitizer.new_col_ids
    )
    
    safe_col_ids = frozenset(c.id for c in table.info.columns if c.is_safe)

    safe_columns = tuple(
        SanitizedColumnInfo(
            id=SanitizedColumnId(c.id.unsafe_name),
            prompt=c.prompt,
            sanitizer_checksum=None,
        ) for c in table.info.columns if c.id in safe_col_ids
    )

    all_columns = sanitized_columns + safe_columns

    # Step 2: Create rowviews that map column names to sanitized/safe values

    sanitized_rows = (
        (sanitizer.get(row.subset(sanitizer.key_col_ids).hash()) for sanitizer in sanitizers)
            for row in table.data.str_rows
    )

    safe_rows = (
        row.subset(safe_col_ids).bless_ids(lambda id: SanitizedColumnId(id.unsafe_name))
            for row in table.data.rows
    )

    all_rows = tuple(
        TableRowView.combine_views(*sanitized, safe)
            for sanitized, safe in zip(sanitized_rows, safe_rows)
    )

    # Step 3: And then you're done!

    return SanitizedTable(
        info=SanitizedTableInfo(
            data_checksum=table.info.data_checksum,
            schema_checksum=table.info.schema_checksum,
            columns=all_columns,
        ),
        data=SanitizedTableData(
            columns_ids=tuple(c.id for c in all_columns),
            rows=all_rows,
        ),
    )

