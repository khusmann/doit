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

def extract_member_data(tf: tarfile.TarFile, name: str):
    member = tf.getmember(name)
    if not member:
        raise Exception("Error: cannot find {} in blob".format(name))
    data = tf.extractfile(member)
    if not data:
        raise Exception("Error: {} is empty in blob".format(name))
    return data.read()

def load_blob_data(tf: tarfile.TarFile, info: BlobInfo):
    match info.source_info:
        case QualtricsSourceInfo():
            schema = extract_member_data(tf, 'schema.json')
            data = extract_member_data(tf, 'data.json')
            return load_unsanitizedtable_qualtrics(schema.decode('utf-8'), data.decode('utf-8'))

def load_blob_info(tf: tarfile.TarFile) -> BlobInfo:
    return BlobInfo.parse_raw(extract_member_data(tf, 'info.json'), encoding="utf8")

### (Impure) IO functions

def read_blob_info(tarfilename: Path | str):
    with tarfile.open(tarfilename, 'r:gz') as tf:
        return load_blob_info(tf)

def read_blob(tarfilename: Path | str):
     with tarfile.open(tarfilename, 'r:gz') as tf:
        info = load_blob_info(tf)
        return load_blob_data(tf, info)

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