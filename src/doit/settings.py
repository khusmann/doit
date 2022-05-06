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

    # General
    output_prefix = "study"

    ### Sources
    source_dir = Path("./build/unsafe/sources")

    def source_table_workdir(self, instrument_id: str) -> Path:
        return self.source_dir / instrument_id

    def blob_from_instrument_name(self, instrument_name: str) -> Path:
        return (self.source_table_workdir(instrument_name) / instrument_name).with_suffix(".tar.gz")

    def blob_bkup_filename(self, instrument_name: str, old_date: datetime) -> Path:
        old_filename = self.blob_from_instrument_name(instrument_name)
        tail = "".join(old_filename.suffixes)
        new_tail = ".{}.tar.gz".format(int(old_date.timestamp()))
        return old_filename.with_name(old_filename.name.replace(tail, new_tail))

    ### Sanitizers

    sanitizer_repo_dir = Path("./build/unsafe/sanitizers")

    def sanitizer_dir_from_instrument_name(self, instrument_name: str) -> Path:
        return self.sanitizer_repo_dir / instrument_name

    #def get_sanitizer_names(self, instrument_id: str) -> t.List[str]:
    #    return [ i.stem for i in self.sanitizer_workdir(instrument_id).glob("*.csv")]

    ### SanitizedTableRepo

    sanitized_repo_dir = Path("./build/safe/sanitized")

    @property
    def sanitized_repo_filename(self):
        return "{}-sanitized.db".format(self.output_prefix)

    @property
    def sanitized_repo_path(self):
        return self.sanitized_repo_dir / self.sanitized_repo_filename

    def sanitized_repo_bkup_path(self, old_date: datetime) -> Path:
        return self.sanitized_repo_path.with_suffix(".{}{}".format(int(old_date.timestamp()), self.sanitized_repo_path.suffix))
        

    # StudySpec

    instrument_dir = Path("./instruments")
    measure_dir = Path("./measures")
    config_file = Path("./study.yaml")

    def instrument_stub_from_instrument_name(self, instrument_name: str):
        return (self.instrument_dir / instrument_name).with_suffix(".yaml.stub")

    # StudyRepo

    study_repo_dir = Path("./build/safe/linked")

    @property
    def study_repo_filename(self):
        return "{}-everything.db".format(self.output_prefix)

    @property
    def study_repo_path(self):
        return self.study_repo_dir / self.study_repo_filename

    def study_repo_bkup_path(self, old_date: datetime) -> Path:
        return self.study_repo_path.with_suffix(".{}{}".format(int(old_date.timestamp()), self.study_repo_path.suffix))

    error_file_path = Path("./doit-errors.csv")

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

