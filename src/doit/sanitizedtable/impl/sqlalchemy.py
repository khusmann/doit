from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
)

Base = declarative_base()

from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTable,
    SanitizedTableInfo,
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

class SqlAlchemyRepo(SanitizedTableRepoReader, SanitizedTableRepoWriter):
    engine: Engine

    def __init__(self, filename: str = ""):
        self.engine = create_engine("sqlite:///{}".format(filename))
        Base.metadata.create_all(self.engine)

    def write_table(self, table: SanitizedTable, name: str):
        session = Session(self.engine)

        table_entry = TableEntrySql(
            name=name,
            data_checksum=table.info.data_checksum,
            schema_checksum=table.info.schema_checksum,
            columns=[
                ColumnEntrySql(
                    name=column.id.name,
                    type="type_field",
                    prompt=column.prompt,
                ) for column in table.info.columns
            ]
        )

        session.add(table_entry) # type: ignore
        session.commit()

    def read_table_info(self, name: str) -> SanitizedTableInfo:
        session = Session(self.engine)

        result: TableEntrySql | None = (
            session.query(TableEntrySql) # type: ignore
                   .filter(TableEntrySql.name == name)
                   .one_or_none()
        )

        if result is None:
            raise Exception("Error: cannot find table {}".format(name))

        return SanitizedTableInfo(
            data_checksum=str(result.data_checksum),
            schema_checksum=str(result.schema_checksum),
            columns=tuple(
                SanitizedColumnInfo(
                    id=SanitizedColumnId(column.name),
                    prompt=column.prompt,
                    sanitizer_checksum=column.sanitizer_checksum,
                ) for column in result.columns
            ),
        )

class TableEntrySql(Base):
    __tablename__ = "__table_entries__"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    data_checksum = Column(String, nullable=False)
    schema_checksum = Column(String, nullable=False)
    columns = relationship(
        "ColumnEntrySql",
        backref="parent_table",
        order_by="ColumnEntrySql.id",
    )

class ColumnEntrySql(Base):
    __tablename__ = "__column_entries"
    id = Column(Integer, primary_key=True)
    parent_table_id = Column(Integer, ForeignKey(TableEntrySql.id), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    prompt = Column(String)
    sanitizer_checksum = Column(String)