from pathlib import Path
import tarfile
from contextlib import contextmanager
import io

from .model import (
    BlobInfo,
    LazyBlob,
    Blob,
    QualtricsSourceInfo,
)

from ..unsanitizedtable.io.qualtrics import (
    load_unsanitizedtable_qualtrics
)

def load_blob_data(blob: LazyBlob):
    match blob.info.source_info:
        case QualtricsSourceInfo():

            schema_lazy = blob.lazy_data.get('schema.json')

            if not schema_lazy:
                raise Exception("Error: cannot find schema.json in qualtrics blob")

            data_lazy = blob.lazy_data.get('data.json')

            if not data_lazy:
                raise Exception("Error: cannot find data.json in qualtrics blob")

            return load_unsanitizedtable_qualtrics(schema_lazy().decode('utf-8'), data_lazy().decode('utf-8'))

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

### (Impure) IO functions

@contextmanager
def open_blob(tarfilename: Path | str):
    tf = tarfile.open(tarfilename, 'r:gz')
    try:
        yield blob_from_tarfile(tf, str(tarfilename))
    finally:
        tf.close()

def write_blob(blob: Blob, tarfilename: str | Path):
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