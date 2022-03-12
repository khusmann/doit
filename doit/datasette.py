# type: ignore
from datasette import hookimpl
from .repo.study import StudyRepo

@hookimpl
def render_cell(value, column, table, database, datasette):
    return list(datasette._doit.measures.values())[0].measure_id

@hookimpl
def startup(datasette):
    async def inner():
        study = StudyRepo().query()
        datasette._doit=study
    return inner