from __future__ import annotations
import typing as t
import yaml
from pydantic import BaseSettings
from pathlib import Path

from ..domain.value import (
    InstrumentName,
    MeasureName,
)

def yaml_config_settings_source(settings: ProjectSettings) -> t.Dict[str, t.Any]:
    encoding = settings.__config__.env_file_encoding
    return yaml.safe_load(Path('config.yaml').read_text(encoding))

class ProjectSettings(BaseSettings):
    # General
    output_prefix = "study"

    # UnsafeTableManager
    unsafe_source_repo_dir = Path("./build/unsafe/sources")

    def unsafe_source_workdir(self, instrument_id: str) -> Path:
        return self.unsafe_source_repo_dir / instrument_id

    def unsafe_source_fileinfo_file(self, instrument_id: str) -> Path:
        return (self.unsafe_source_workdir(instrument_id) / instrument_id).with_suffix(".json")

    def unsafe_source_fetchinfo_file(self, instrument_id: str) -> Path:
        return (self.unsafe_source_workdir(instrument_id) / instrument_id).with_suffix(".fetch.json")

    # SourceTableManager
    safe_source_repo_dir = Path("./build/safe/sanitized")

    @property
    def source_database_filename(self):
        return "{}-sanitized.db".format(self.output_prefix)

    def source_database_path(self):
        return self.safe_source_repo_dir / self.source_database_filename

    # StudySpecManager
    instrument_dir = Path("./instruments")
    measure_dir = Path("./measures")
    config_file = Path("./study.yaml")

    def instrument_file(self, instrument_id: InstrumentName) -> Path:
        return (self.instrument_dir / instrument_id).with_suffix(".yaml")

    def measure_file(self, measure_id: MeasureName) -> Path:
        return (self.measure_dir / measure_id).with_suffix(".yaml")

    # StudyRepoManager
    study_repo_dir = Path("./build/safe/linked")

    @property
    def everything_database_filename(self):
        return "{}-everything.db".format(self.output_prefix)

    def everything_database_path(self):
        return self.study_repo_dir / self.everything_database_filename

    ###

    class Config(BaseSettings.Config):
        env_file_encoding = 'utf-8'

        @classmethod
        def customise_sources(
            cls,
            init_settings: t.Any,
            env_settings: t.Any,
            file_secret_settings: t.Any,
        ):
            return (
                init_settings,
                yaml_config_settings_source,
                env_settings,
                file_secret_settings,
            )

