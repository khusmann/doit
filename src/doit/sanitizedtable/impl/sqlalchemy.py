from sqlalchemy import create_engine

from sqlalchemy.engine import Engine

from ..model import (
    SanitizedTable,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

def new_sqlalchemy_repo(filename: str):
    db_url = "sqlite:///{}".format(filename)
    repo = SqlAlchemyRepo(db_url)
    return SanitizedTableRepoWriter(
        write_table=repo.write_table,
    )

def open_sqlalchemy_repo(filename: str):
    db_url = "sqlite:///{}".format(filename)
    return SanitizedTableRepoReader(
        read_table=SqlAlchemyRepo(db_url).read_table
    )

class SqlAlchemyRepo:
    engine: Engine

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)

    def write_table(self, table: SanitizedTable, name: str):
        pass

    def read_table(self, name: str) -> SanitizedTable:
        return SanitizedTable(
            info=SanitizedTableInfo(
                data_checksum="",
                schema_checksum="",
                columns=(),
            ),
            data=SanitizedTableData(
                column_ids=(),
                rows=(),
            )
        )

