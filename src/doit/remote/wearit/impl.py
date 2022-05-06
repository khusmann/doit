import typing as t

from pathlib import Path

from datetime import datetime, timezone

from ..model import (
    BlobInfo,
    SourceColumnInfo,
    WearitSourceInfo,
    Blob,
)

from ...unsanitizedtable.io.wearit import (
    load_unsanitizedtable_wearit,
)



def fetch_wearit_blob(data_path: str | Path, progress_callback: t.Callable[[int], None] = lambda _: None):
    data_path = Path(data_path)
    schema_path = data_path.with_suffix(".json")

    table_schema = schema_path.read_text()
    table_data = data_path.read_text()

    table = load_unsanitizedtable_wearit(table_schema, table_data)
    
    progress_callback(100)

    info = BlobInfo(
        fetch_date_utc=datetime.now(timezone.utc),
        title=table.source_title,
        source_info=WearitSourceInfo(
            type='wearit',
            data_checksum=table.data_checksum,
            schema_checksum=table.schema_checksum,
        ),
        columns=tuple(
            SourceColumnInfo(
                name=c.id.unsafe_name,
                prompt=c.prompt,
                ) for c in table.schema
        )
    )

    return Blob(
        info=info,
        lazydata={
            "schema.json": lambda: table_schema.encode('utf-8'),
            "data.csv": lambda: table_data.encode('utf-8'),
        }
    )    
    