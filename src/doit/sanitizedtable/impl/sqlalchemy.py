import typing as t
import json

from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    insert,
)

from sqlalchemy.orm import (
    Session,
    relationship,
)

from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base

from doit.common import TableValue

Base = declarative_base()

from ...common import (
    Omitted,
    Some,
    ErrorValue,
    Redacted,
    TableRowView,
)

from ..model import (
    SanitizedColumnId,
    SanitizedColumnInfo,
    SanitizedTable,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

class SqlAlchemyRepo(SanitizedTableRepoReader, SanitizedTableRepoWriter):
    engine: Engine
    datatables: t.Dict[str, Table]

    def __init__(self, filename: str = ""):
        self.engine = create_engine("sqlite:///{}".format(filename))
        self.datatables = {}
        Base.metadata.create_all(self.engine)

    def write_table(self, table: SanitizedTable, name: str):
        self.datatables[name] = sql_from_tableinfo(table.info, name)
        self.datatables[name].create(self.engine)

        session = Session(self.engine)

        table_entry = TableEntrySql(
            name=name,
            data_checksum=table.info.data_checksum,
            schema_checksum=table.info.schema_checksum,
            columns=[
                ColumnEntrySql(
                    name=column.id.name,
                    type=column.type,
                    prompt=column.prompt,
                ) for column in table.info.columns
            ]
        )

        session.add(table_entry) # type: ignore

        render_rows = [
            tuple(render_value(c, row.get(c.id)) for c in table.info.columns) for row in table.data.rows
        ]

        session.execute( # type: ignore
            insert(self.datatables[name]).values(render_rows)
        )        

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
                    type=column.type,
                ) for column in result.columns
            ),
        )

    def read_table(self, name: str) -> SanitizedTable:
        info = self.read_table_info(name)

        if name not in self.datatables:
            self.datatables[name] = sql_from_tableinfo(info, name)

        session = Session(self.engine)

        raw_rows = session.query(self.datatables[name]).all()

        column_ids = tuple(c.id for c in info.columns)

        data = SanitizedTableData(
            column_ids=column_ids,
            rows=tuple(
                TableRowView({
                    cid: Some(v) if v else Omitted() for cid, v in zip(column_ids, row)
                }) for row in raw_rows
            )
        )

        return SanitizedTable(
            info=info,
            data=data,
        )
        

COLUMN_TYPE_LOOKUP = {
    'ordinal': Integer,
    'multiselect': String,
    'text': String,
}

def render_value(column: SanitizedColumnInfo, v: TableValue):
    if isinstance(v, Omitted):
        return None

    if isinstance(v, ErrorValue):
        print("Encountered error value: {}".format(v))
        return None

    if column.type == 'text':
        match v:
            case Some(value=value):
                return str(value)
            case Redacted():
                return "__REDACTED__"

    if isinstance(v, Redacted):
        print("Unexpected redacted value in a non-text column")
        return None

    match column.type:
        case 'ordinal':
            return int(v.value)
        case 'multiselect':
            return json.dumps(v.value)

def sql_from_tableinfo(table: SanitizedTableInfo, name: str):
    return Table(
        name,
        Base.metadata,
        *[
            Column(
                i.id.name,
                COLUMN_TYPE_LOOKUP[i.type],
            ) for i in table.columns
        ]
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