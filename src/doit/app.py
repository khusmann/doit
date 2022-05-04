import typing as t
from pathlib import Path
from datetime import datetime, timezone

from .sanitizer.model import SanitizerUpdate, TableSanitizer

from .unsanitizedtable.model import UnsanitizedTable

from .common.table import TableErrorReport

from .remote.fetch import ( # TODO: put these imports into the functions that call them
    fetch_blob,
    get_listing,
)

from .remote.blob import (
    load_blob,
    write_blob,
    read_blob_info,
    read_blob,
)

from .remote.model import (
    Blob,
    LocalTableListing,
    RemoteTableListing,
)

from .study.spec import StudySpec
from .study.io import load_studyspec_str

def add_source(
    instrument_name: str,
    uri: str,
    progress_callback: t.Callable[[int], None],
    blob_from_instrument_name: t.Callable[[str], Path],
) -> Blob:
    blob = fetch_blob(uri, progress_callback)
    write_blob(blob, blob_from_instrument_name(instrument_name))
    return blob
    
def fetch_source(
    instrument_name: str,
    progress_callback: t.Callable[[int], None],
    blob_from_instrument_name: t.Callable[[str], Path],
    blob_bkup_filename: t.Callable[[str, datetime], Path],
) -> UnsanitizedTable:

    info = read_blob_info(blob_from_instrument_name(instrument_name))

    filename = blob_from_instrument_name(instrument_name)

    bkup_filename = filename.rename(blob_bkup_filename(instrument_name, info.fetch_date_utc))

    try:
        new_blob = add_source(
            instrument_name,
            info.source_info.uri,
            progress_callback,
            blob_from_instrument_name,
        )
    except Exception as e:
        bkup_filename.rename(filename)
        raise e

    if info.source_info == new_blob.info.source_info:
        bkup_filename.unlink()

    return load_blob(new_blob)

def load_unsanitizedtable(
    instrument_name: str,
    blob_from_instrument_name: t.Callable[[str], Path],
):
    return read_blob(blob_from_instrument_name(instrument_name))

def load_source_info(
    instrument_name: str,
    blob_from_instrument_name: t.Callable[[str], Path],
):
    return read_blob_info(blob_from_instrument_name(instrument_name))

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

def load_table_sanitizer(
    instrument_name: str,
    sanitizer_dir_from_instrument_name: t.Callable[[str], Path],
):
    from .sanitizer.io import load_sanitizer_csv
    return TableSanitizer(
        table_name=instrument_name,
        sanitizers=tuple(
            load_sanitizer_csv(i.read_text(), i.stem)
                for i in sanitizer_dir_from_instrument_name(instrument_name).glob("*.csv")
        )
    )

def update_sanitizer(
    instrument_name: str,
    sanitizer_updates: t.Sequence[SanitizerUpdate],
    sanitizer_dir_from_instrument_name: t.Callable[[str], Path],
):
    from .sanitizer.io import write_sanitizer_update

    workdir = sanitizer_dir_from_instrument_name(instrument_name)
    workdir.mkdir(parents=True, exist_ok=True)

    for update in sanitizer_updates:
        sanitizer_path = (workdir / update.name).with_suffix(".csv")
        if update.new:
            if sanitizer_path.exists():
                raise Exception("Error: attempting to create new sanitizer but {} already exists".format(sanitizer_path))
            with open(sanitizer_path, "w", newline='') as f:
                write_sanitizer_update(f, update, True)
        else:
            if not sanitizer_path.exists():
                raise Exception("Error: attempting to update sanitizer but {} does not exist".format(sanitizer_path))
            with open(sanitizer_path, "a", newline='') as f:
                write_sanitizer_update(f, update, False)

def new_sanitizedtable_repo(
    sanitized_repo_name: Path,
    sanitized_repo_bkup_path: t.Callable[[datetime], Path],
):
    if sanitized_repo_name.exists():
        sanitized_repo_name.rename(sanitized_repo_bkup_path(datetime.now(timezone.utc)))
    from .sanitizedtable.sqlalchemy.impl import SqlAlchemyRepo
    return SqlAlchemyRepo.new(str(sanitized_repo_name))

def open_sanitizedtable_repo(
    sanitized_repo_name: Path,
):
    if not sanitized_repo_name.exists():
        raise Exception("Error: {} does not exist".format(sanitized_repo_name))
    from .sanitizedtable.sqlalchemy.impl import SqlAlchemyRepo
    return SqlAlchemyRepo.open(str(sanitized_repo_name))

def new_study_repo(
    study_spec: StudySpec,
    study_repo_name: Path,
    study_repo_bkup_path: t.Callable[[datetime], Path],
):
    if study_repo_name.exists():
        study_repo_name.rename(study_repo_bkup_path(datetime.now(timezone.utc)))
    from .study.sqlalchemy.impl import SqlAlchemyRepo
    return SqlAlchemyRepo.new(study_spec, str(study_repo_name))

def load_study_spec(
    config_file: Path,
    instrument_dir: Path,
    measure_dir: Path,
) -> StudySpec:
    import yaml
    return load_studyspec_str(
        config=config_file.read_text(),
        measures={ i.stem: i.read_text() for i in measure_dir.glob("*.yaml")},
        instruments={ i.stem: i.read_text() for i in instrument_dir.glob("*.yaml")},
        parser=yaml.safe_load,
    )

def write_errors(
    report: TableErrorReport,
    error_file_path: Path
):
    from .common.table import write_error_report
    with open(error_file_path, "w") as f:
        write_error_report(f, report)
