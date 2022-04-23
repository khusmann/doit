from __future__ import annotations
import typing as t
import yaml
from pydantic import BaseSettings
from pathlib import Path
from datetime import datetime

def yaml_config_settings_source(settings: AppSettings) -> t.Dict[str, t.Any]:
    encoding = settings.__config__.env_file_encoding
    try:
        return yaml.safe_load(Path('config.yaml').read_text(encoding))
    except:
        return {}

class AppSettings(BaseSettings):

    ### New Settings
    def blob_from_instrument_name(self, instrument_name: str) -> Path:
        return (self.unsafe_table_workdir(instrument_name) / instrument_name).with_suffix(".tar.gz")

    def blob_bkup_filename(self, instrument_name: str, old_date: datetime) -> Path:
        old_filename = self.blob_from_instrument_name(instrument_name)
        tail = "".join(old_filename.suffixes)
        new_tail = ".{}.tar.gz".format(int(old_date.timestamp()))
        return old_filename.with_name(old_filename.name.replace(tail, new_tail))

    # General
    output_prefix = "study"

    # UnsafeTableManager
    unsafe_source_repo_dir = Path("./build/unsafe/sources")

    def unsafe_table_workdir(self, instrument_id: str) -> Path:
        return self.unsafe_source_repo_dir / instrument_id

    def unsafe_table_fileinfo_file(self, instrument_id: str) -> Path:
        return (self.unsafe_table_workdir(instrument_id) / instrument_id).with_suffix(".json")

    def unsafe_table_fetchinfo_file(self, instrument_id: str) -> Path:
        return (self.unsafe_table_workdir(instrument_id) / instrument_id).with_suffix(".fetch.json")

    def get_unsafe_table_names(self) -> t.List[str]:
        return [ i.name for i in self.unsafe_source_repo_dir.iterdir() if i.is_dir() and i.name[0] != '.' ]

    # SanitizerManager

    sanitizer_repo_dir = Path("./build/unsafe/sanitizers")

    def sanitizer_workdir(self, instrument_id: str) -> Path:
        return self.sanitizer_repo_dir / instrument_id

    def sanitizer_file(self, instrument_id: str, sanitizer_id: str) -> Path:
        return ((self.sanitizer_workdir(instrument_id) / sanitizer_id).with_suffix(".csv"))

    def get_sanitizer_names(self, instrument_id: str) -> t.List[str]:
        return [ i.stem for i in self.sanitizer_workdir(instrument_id).glob("*.csv")]

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

    def instrument_file(self, instrument_id: str) -> Path:
        return (self.instrument_dir / instrument_id).with_suffix(".yaml")

    def measure_file(self, measure_id: str) -> Path:
        return (self.measure_dir / measure_id).with_suffix(".yaml")

    def get_instrument_spec_names(self) -> t.List[str]:
        return [ i.stem for i in self.instrument_dir.glob("*.yaml")]

    def get_measure_spec_names(self) -> t.List[str]:
        return [ i.stem for i in self.measure_dir.glob("*.yaml")]

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

