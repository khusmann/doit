from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import (
    Column,
    Integer,
    String,
)

Base = declarative_base()

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
        Base.metadata.create_all(self.engine)

    def write_table(self, table: SanitizedTable, name: str):
        session = Session(self.engine)

        table_entry = TableEntrySql(
            name=name,
            data_checksum=table.info.data_checksum,
            schema_checksum=table.info.schema_checksum,
        )

        session.add(table_entry) # type: ignore

        session.commit()

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

class TableEntrySql(Base):
    __tablename__ = "__table_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    data_checksum = Column(String, nullable=False)
    schema_checksum = Column(String, nullable=False)