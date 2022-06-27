from doit.study.repo import StudyRepoReader
import pandas as pd
from pathlib import Path

from doit.study.view import ColumnRawView
from .study.spec import PackageSpec, PivotTransformSpec, RenameTransformSpec
from .study.sqlalchemy.impl import SqlAlchemyRepo
from itertools import groupby
from functools import reduce

def key_func(c: ColumnRawView):
    return (c.table_name, c.indices)

def package_data(
    spec: PackageSpec,
    repo: StudyRepoReader,
    out_dir: Path,
):
    assert(isinstance(repo, SqlAlchemyRepo))
    raw_columns = sorted(repo.query_column_raw(spec.items), key=key_func)

    selections = tuple(
        pd.read_sql(
            "SELECT {} from {}".format(",".join((
                *('"'+idx+'"' for idx in indices),
                *('"'+c.name+'"' for c in cols),
            )), table),
            index_col=list(indices),
            con=repo.engine,
        ) for (table, indices), cols in groupby(raw_columns, lambda c: (c.table_name, c.indices))
    )

    result = reduce(lambda acc, x: acc.join(x, how='outer'), reversed(selections)) # type: ignore

    for t in spec.transforms:
        match t:
            case PivotTransformSpec():
                result = result.unstack(t.index) # type: ignore
                result.columns = [f'{x}.{y}' for x,y in result.columns] # type: ignore
            case RenameTransformSpec():
                result.rename( # type: ignore
                    columns=t.map, inplace=True
                )
                result.rename( # type: ignore
                    index=t.map, inplace=True
                )

    out_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_dir / spec.output) # type: ignore