from __future__ import annotations
import typing as t

from sqlalchemy import (
    create_engine,
    Table,
    insert,
    select,
)

from sqlalchemy.orm import (
    Session,
)

from sqlalchemy.engine import Engine

from .model import (
    Base,
    TableEntrySql,
)

from .conversion import (
    sql_from_tableinfo,
    sqlschema_from_tableinfo,
    tableinfo_from_sql,
    render_value,
)

from ...common import (
    Omitted,
    Some,
    TableRowView,
)

from ..model import (
    SanitizedTable,
    SanitizedTableData,
    SanitizedTableInfo,
    SanitizedTableRepoReader,
    SanitizedTableRepoWriter,
)

T = t.TypeVar('T')

class SessionWrapper(Session):
    def __init__(self, engine: Engine):
        super().__init__(engine)

    def get_by_name(self, type: t.Type[T], name: str) -> T:
        entry: T | None = self.execute( # type: ignore
            select(type).filter_by(name=name)
        ).scalars().one_or_none()

        if entry is None:
            raise Exception("Error: Entry named {} not found".format(name))

        return entry

    def get_all(self, type: t.Type[T]) -> t.Sequence[T]:
        return self.execute( # type: ignore
            select(type)
        ).scalars().all()


class SqlAlchemyRepo(SanitizedTableRepoReader, SanitizedTableRepoWriter):
    engine: Engine
    datatables: t.Dict[str, Table]

    def __init__(self, engine: Engine, datatables: t.Mapping[str, Table]):
        self.engine = engine
        self.datatables = dict(datatables)

    @classmethod
    def create_engine(cls, filename: str):
        return create_engine("sqlite:///{}".format(filename))

    @classmethod
    def new(cls, filename: str = ""):
        engine = cls.create_engine(filename)
        Base.metadata.create_all(engine)
        return cls(engine, {})

    @classmethod
    def open(cls, filename: str = ""):
        engine = cls.create_engine(filename)

        session = SessionWrapper(engine)

        datatables = {
            str(i.name): sqlschema_from_tableinfo(tableinfo_from_sql(i), str(i.name))
                for i in session.get_all(TableEntrySql)
        }

        return cls(engine, datatables)

    def write_table(self, table: SanitizedTable, name: str):
        self.datatables[name] = sqlschema_from_tableinfo(table.info, name)
        self.datatables[name].create(self.engine)

        session = Session(self.engine)

        session.add( # type: ignore
            sql_from_tableinfo(table.info, name)
        ) 

        session.execute( # type: ignore
            insert(self.datatables[name]).values([
                tuple(render_value(c, row.get(c.id)) for c in table.info.columns)
                    for row in table.data.rows
            ])
        )        

        session.commit()

    def read_tableinfo(self, name: str) -> SanitizedTableInfo:
        return tableinfo_from_sql(SessionWrapper(self.engine).get_by_name(TableEntrySql, name))

    def read_table(self, name: str) -> SanitizedTable:
        if name not in self.datatables:
            raise Exception("Error: No schema for {}".format(name))
        
        session = SessionWrapper(self.engine)
        
        info = tableinfo_from_sql(session.get_by_name(TableEntrySql, name))

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
        

