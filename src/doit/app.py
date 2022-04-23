#import typing as t
from pathlib import Path
from pydantic import BaseModel
from .settings import AppSettings

class QualtricsSourceInfo(BaseModel):
    uri: str
    title: str
    data_path: Path
    schema_path: Path

class LocalCsvSourceInfo(BaseModel):
    title: str
    path: Path

TableSourceInfo = QualtricsSourceInfo | LocalCsvSourceInfo

defaults = AppSettings()

def add_instrument(instrument_name: str, uri: str) -> TableSourceInfo:
    return _fetch_instrument(
        uri=uri,
        save_data_path=defaults.unsafe_table_workdir(instrument_name) / "qualtrics-data.json",
        save_schema_path=defaults.unsafe_table_workdir(instrument_name) / "qualtrics-schema.json",
    )

def _fetch_instrument(
    uri: str,
    save_data_path: Path,
    save_schema_path: Path,
) -> TableSourceInfo:
    from urllib.parse import urlparse, ParseResult

    save_data_path.parent.mkdir(exist_ok=True, parents=True)
    save_schema_path.parent.mkdir(exist_ok=True, parents=True)

    match urlparse(uri):
        case ParseResult(scheme="qualtrics", netloc=remote_id):
            from .remote.qualtrics import fetch_qualtrics_source
            from .unsanitizedtable.io.qualtrics import load_unsanitizedtable_qualtrics

            fetch_qualtrics_source(remote_id, save_data_path, save_schema_path)

            with open(save_data_path, 'r') as data_json, open(save_schema_path, 'r') as schema_json:
                table = load_unsanitizedtable_qualtrics(schema_json.read(), data_json.read())

                sourceinfo = QualtricsSourceInfo(
                    uri=uri,
                    title=table.source_title,
                    data_path=save_data_path,
                    schema_path=save_schema_path,
                )



        case _:
            raise Exception("Unrecognized uri: {}".format(uri))
    
    # TODO: write TableSourceInfo
    #with open(self.settings.unsafe_table_fileinfo_file(instrument_id), 'w') as f:
    #    f.write(file_info.json())

    return sourceinfo

def fetch_instrument(instrument_name: str):
    # Read TableSourceInfo
    # _fetch_instrument()
    pass