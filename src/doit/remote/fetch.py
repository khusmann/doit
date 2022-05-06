import typing as t
from pathlib import Path
from urllib.parse import urlparse, ParseResult

from .model import Blob

### (Impure) Fetching functions

def get_listing(remote_service: str):
    match remote_service:
        case "qualtrics":
            from .qualtrics.impl import fetch_qualtrics_listing
            return fetch_qualtrics_listing()
        case _:
            raise Exception("Unrecognized service: {}".format(remote_service))

def fetch_blob(uri: str | Path, progress_callback: t.Callable[[int], None] = lambda _: None) -> Blob:
    match urlparse(str(uri)):
        case ParseResult(scheme="qualtrics", netloc=remote_id):
            from .qualtrics.impl import fetch_qualtrics_blob
            return fetch_qualtrics_blob(remote_id, progress_callback)
        case ParseResult(scheme="wearit", path=data_path):
            from .wearit.impl import fetch_wearit_blob
            return fetch_wearit_blob(data_path, progress_callback)
        case _:
            raise Exception("Unrecognized uri: {}".format(uri))

