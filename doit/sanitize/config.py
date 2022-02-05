import yaml
import typing as t
from pathlib import Path

ENV_FILENAME = Path(".env.yaml")

def load_defaults() -> t.Mapping[str, str]:
    if ENV_FILENAME.is_file():
        with open(ENV_FILENAME, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
    return dict()

