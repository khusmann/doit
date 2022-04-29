from pathlib import Path
import tarfile
import io

from .model import (
    BlobInfo,
    Blob,
    QualtricsSourceInfo,
)

from ..unsanitizedtable.io.qualtrics import (
    load_unsanitizedtable_qualtrics
)

def load_blob(blob: Blob):
    match blob.info.source_info:
        case QualtricsSourceInfo():
            schema = blob.data['schema.json']()
            data = blob.data['data.json']()
            return load_unsanitizedtable_qualtrics(schema.decode('utf-8'), data.decode('utf-8'))

def extract_member_data_fn(tf: tarfile.TarFile, member: tarfile.TarInfo):
    def inner():
        data = tf.extractfile(member)
        if not data:
            raise Exception("Error: {} is empty in blob".format(member.name))
        return data.read()
    return inner

def blob_from_tar(tf: tarfile.TarFile) -> Blob:
    info_member = tf.getmember('info.json')
    info_data = tf.extractfile(info_member)
    if not info_data:
        raise Exception("Error: info.json missing in blob")

    info = BlobInfo.parse_raw(info_data.read(), encoding="utf8")
    return Blob(
        info=info,
        data={
            m.name: extract_member_data_fn(tf, m) for m in tf.getmembers() if m.name != "info.json"
        }
    )

### (Impure) IO functions

def read_blob_info(tarfilename: Path | str):
    with tarfile.open(tarfilename, 'r:gz') as tf:
        return blob_from_tar(tf).info

def read_blob(tarfilename: Path | str):
     with tarfile.open(tarfilename, 'r:gz') as tf:
        return load_blob(blob_from_tar(tf))

def write_blob(blob: Blob, tarfilename: str | Path):
    tarfilename = Path(tarfilename)

    if tarfilename.exists():
        raise Exception("Error: {} already exists!".format(tarfilename))

    tarfilename.parent.mkdir(exist_ok=True, parents=True)
    
    with tarfile.open(tarfilename, 'w:gz') as tf:
        blob_entries = (*blob.data.items(), ("info.json", lambda: blob.info.json().encode('utf-8')))
        for filename, lazycontent in blob_entries:
            content = lazycontent()
            ifo = tarfile.TarInfo(filename)
            ifo.size = len(content)
            tf.addfile(ifo, io.BytesIO(content))   