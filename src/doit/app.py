import typing as t
from pathlib import Path
from datetime import datetime, timezone

from .remote.io import (
    fetch_blob,
    save_blob,
    open_blob,
    load_blob,
    uri_from_blobinfo,
    get_listing,
)

from .remote.model import (
    Blob,
    BlobInfo,
    LocalTableListing,
    RemoteTableListing,
)

from .sanitizedtable.io import (
    new_sanitizedtable_repo,
)

def add_source(
    instrument_name: str,
    uri: str,
    progress_callback: t.Callable[[int], None],
    blob_from_instrument_name: t.Callable[[str], Path],
) -> Blob:
    blob = fetch_blob(uri, progress_callback)
    save_blob(blob, blob_from_instrument_name(instrument_name))
    return blob
    
def fetch_source(
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
        new_blob = add_source(
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

def load_unsanitizedtable(
    instrument_name: str,
    blob_from_instrument_name: t.Callable[[str], Path],
):
    with open_blob(blob_from_instrument_name(instrument_name)) as blob:
        return load_blob(blob)

def load_source_info(
    instrument_name: str,
    blob_from_instrument_name: t.Callable[[str], Path],
):
    with open_blob(blob_from_instrument_name(instrument_name)) as blob:
        return blob.info

def get_remote_source_listing(
    remote_service: str
) -> t.Tuple[RemoteTableListing, ...]:
    return get_listing(remote_service)

def get_local_source_listing(
    source_workdir: Path,
    blob_from_instrument_name: t.Callable[[str], Path],
) -> t.Tuple[LocalTableListing, ...]:
    sources = tuple(
        i.name
            for i in source_workdir.iterdir()
                if i.is_dir() and i.name[0] != '.' and blob_from_instrument_name(i.name).exists()
    )
    return tuple(
        LocalTableListing(
            name=source,
            title=load_source_info(source, blob_from_instrument_name).title,
        ) for source in sources
    )

def new_sanitizedtable_repo_version(
    sanitized_repo_name: Path,
    sanitized_repo_bkup_path: t.Callable[[datetime], Path]
):
    if sanitized_repo_name.exists():
        sanitized_repo_name.rename(sanitized_repo_bkup_path(datetime.now(timezone.utc)))
    return new_sanitizedtable_repo(sanitized_repo_name)

#def source_listing(
#    source_workdir: str
#) -> t.Tuple[str]:
#    dirs = 