from pathlib import Path

def new_sanitizedtable_repo(filename: Path | str, repo_impl: str = "sqlalchemy"):
    filename = Path(filename)

    if filename.exists():
        raise Exception("Error: {} already exists".format(filename))

    match repo_impl:
        case "sqlalchemy":
            from .impl.sqlalchemy import new_sqlalchemy_repo
            return new_sqlalchemy_repo(str(filename))
        case _:
            raise Exception("Unknown impl: {}".format(repo_impl))

def open_sanitizedtable_repo(filename: Path | str, repo_impl: str = "sqlalchemy"):
    filename = Path(filename)

    if not filename.exists():
        raise Exception("Error: {} doesn't exist".format(filename))

    match repo_impl:
        case "sqlalchemy":
            from .impl.sqlalchemy import open_sqlalchemy_repo
            return open_sqlalchemy_repo(str(filename))
        case _:
            raise Exception("Unknown impl: {}".format(repo_impl))