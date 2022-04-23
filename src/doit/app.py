import typing as t
from pathlib import Path
from datetime import datetime

from .remote.service import (
    fetch_blob,
    save_blob,
    open_blob,
    load_blob,
    uri_from_blobinfo,
)

from .remote.model import (
    Blob,
    BlobInfo,
)

def add_instrument(
    instrument_name: str,
    uri: str,
    progress_callback: t.Callable[[int], None],
    blob_from_instrument_name: t.Callable[[str], Path],
) -> Blob:
    blob = fetch_blob(uri, progress_callback)
    save_blob(blob, blob_from_instrument_name(instrument_name))
    return blob
    
def fetch_instrument(
    instrument_name: str,
    progress_callback: t.Callable[[int], None],
    blob_from_instrument_name: t.Callable[[str], Path],
    blob_bkup_filename: t.Callable[[str, datetime], Path],
) -> t.Optional[BlobInfo]:

    with open_blob(blob_from_instrument_name(instrument_name)) as blob:
        info = blob.info

    filename = blob_from_instrument_name(instrument_name)

    bkup_filename = filename.rename(blob_bkup_filename(instrument_name, info.fetch_date_utc))

    try:
        new_blob = add_instrument(
            instrument_name,
            uri_from_blobinfo(info),
            progress_callback,
            blob_from_instrument_name,
        )
    except Exception as e:
        bkup_filename.rename(filename)
        raise e

    if info.source_info == new_blob.info.source_info:
        bkup_filename.unlink()
        return None
    else:
        return new_blob.info

def load_instrument(
    instrument_name: str,
    blob_from_instrument_name: t.Callable[[str], Path],
):
    with open_blob(blob_from_instrument_name(instrument_name)) as blob:
        return load_blob(blob)