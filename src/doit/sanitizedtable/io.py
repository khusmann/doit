from pathlib import Path

from .model import (
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

def new_sanitizedtable_repo(filename: Path | str = "", repo_impl: str = "sqlalchemy") -> SanitizedTableRepoWriter:
    filename = Path(filename)

    if filename.exists():
        raise Exception("Error: {} already exists".format(filename))

    match repo_impl:
        case "sqlalchemy":
            from .impl.sqlalchemy import SqlAlchemyRepo
            return SqlAlchemyRepo(str(filename))
        case _:
            raise Exception("Unknown impl: {}".format(repo_impl))

def open_sanitizedtable_repo(filename: Path | str = "", repo_impl: str = "sqlalchemy") -> SanitizedTableRepoReader:
    filename = Path(filename)

    if not filename.exists():
        raise Exception("Error: {} doesn't exist".format(filename))

    match repo_impl:
        case "sqlalchemy":
            from .impl.sqlalchemy import SqlAlchemyRepo
            return SqlAlchemyRepo(str(filename))
        case _:
            raise Exception("Unknown impl: {}".format(repo_impl))