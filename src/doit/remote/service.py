import typing as t
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse, ParseResult
import tarfile
import io

from .model import (
    BlobInfo,
    LazyBlob,
    Blob,
    QualtricsSourceInfo,
)

from ..unsanitizedtable.model import (
    UnsanitizedTable
)

def fetch_blob(uri: str | Path, progress_callback: t.Callable[[int], None] = lambda _: None) -> Blob:
    match urlparse(str(uri)):
        case ParseResult(scheme="qualtrics", netloc=remote_id):
            from .qualtrics import fetch_qualtrics_blob
            return fetch_qualtrics_blob(remote_id, progress_callback)
        case _:
            raise Exception("Unrecognized uri: {}".format(uri))

def uri_from_blobinfo(info: BlobInfo):
    match info.source_info:
        case QualtricsSourceInfo(remote_id=remote_id):
            return "qualtrics://" + remote_id

def load_blob(blob: LazyBlob) -> UnsanitizedTable:
    match blob.info.source_info:
        case QualtricsSourceInfo():
            from .qualtrics import load_qualtrics_blob_data
            return load_qualtrics_blob_data(blob.lazy_data)

def save_blob(blob: Blob, tarfilename: str | Path):
    tarfilename = Path(tarfilename)

    if tarfilename.exists():
        raise Exception("Error: {} already exists!".format(tarfilename))

    tarfilename.parent.mkdir(exist_ok=True, parents=True)
    
    with tarfile.open(tarfilename, 'w:gz') as tf:
        blob_entries = (*blob.data.items(), ("info.json", blob.info.json().encode('utf-8')))
        for filename, content in blob_entries:
            ifo = tarfile.TarInfo(filename)
            ifo.size = len(content)
            tf.addfile(ifo, io.BytesIO(content))   

def blob_from_tarfile(tf: tarfile.TarFile, tarfilename: str) -> LazyBlob:
    info_member = tf.getmember("info.json")

    if info_member is None:
        raise Exception("Error: info.json missing from {}".format(tarfilename))

    member = tf.extractfile(info_member)

    if member is None:
        raise Exception("Error: unable to extract info.json from {}".format(tarfilename))

    info = BlobInfo.parse_raw(member.read(), encoding="utf8")

    def lazy_load(member: tarfile.TarInfo):
        def impl():
            m = tf.extractfile(member)
            if not m:
                raise Exception("Error: {} in tar file has no data".format(member.name))
            return m.read()
        return impl

    lazy_data = {
        member.name: lazy_load(member)
            for member in tf.getmembers()
                if member.name != "info.json"
    }

    return LazyBlob(
        info=info,
        lazy_data=lazy_data,
    )

@contextmanager
def open_blob(tarfilename: Path | str):
    tf = tarfile.open(tarfilename, 'r:gz')
    try:
        yield blob_from_tarfile(tf, str(tarfilename))
    finally:
        tf.close()
