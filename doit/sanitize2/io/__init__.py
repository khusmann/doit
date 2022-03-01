import typing as t
from pydantic import BaseModel
from pathlib import Path

import os

from .instrumentsourceimpl import InstrumentSourceImpl, TestSourceImpl
from .qualtrics import QualtricsSourceImpl, QualtricsApiImpl

from time import time
from urllib.parse import urlparse

####
SOURCE_PATH = Path("./build/unsafe/sources")

def SOURCE_WORKDIR(instrument_id: str):
    return SOURCE_PATH / instrument_id

def SOURCE_INFO_FILENAME(instrument_id: str):
    return (SOURCE_WORKDIR(instrument_id) / instrument_id).with_suffix(".json")
####

_impl_map: t.Mapping[str, InstrumentSourceImpl] = {
    'qualtrics': QualtricsSourceImpl(
        QualtricsApiImpl(
            api_key=os.environ['QUALTRICS_API_KEY'],
            data_center=os.environ['QUALTRICS_DATA_CENTER'],
        )
    ),
    'test': TestSourceImpl() 
}

def fetch_remote_instrument_desc(remote: str):
    return _impl_map[remote].fetch_available_desc()

class InstrumentSource(BaseModel):
    instrument_id: str
    uri: str

    def fetch(self):
        _impl_map[urlparse(self.uri).scheme].fetch(self.workdir(), self.uri)

    def load_data(self):
        return _impl_map[urlparse(self.uri).scheme].load_data(self.workdir(), self.uri)

    def rm(self):
        oldfile = self.workdir()
        newfile = oldfile.with_name(".{}.{}".format(oldfile.name, int(time())))
        oldfile.rename(newfile)

    def save(self):
        self.workdir().mkdir(exist_ok=True)
        with open(self.info_filename(), 'w') as f:
            f.write(self.json())

    def info_filename(self):
        return SOURCE_INFO_FILENAME(self.instrument_id)

    def workdir(self):
        return SOURCE_WORKDIR(self.instrument_id)

    @classmethod
    def load(cls, instrument_id: str):
        return InstrumentSource.parse_file(SOURCE_INFO_FILENAME(instrument_id))

    @classmethod
    def load_all(cls):
        return { i.name: cls.load(i.name) for i in SOURCE_PATH.iterdir() if i.is_dir() and i.name[0] != '.' }