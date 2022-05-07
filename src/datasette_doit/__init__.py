# type: ignore
from datasette import hookimpl
from pathlib import Path
from datasette.utils.asgi import Response
from functools import cache
import markupsafe
import click
import html

import uvicorn
from doit import app, settings
from doit.study.sqlalchemy.impl import SqlAlchemyRepo

defaults = settings.AppSettings()



@hookimpl
def menu_links(datasette, actor):
    return [
        {"href": datasette.urls.path("/-/instruments"), "label": "Instruments"},
        {"href": datasette.urls.path("/-/measures"), "label": "Measures"},
    ]

@hookimpl
def prepare_jinja2_environment(env):
    env.filters['base_id'] = lambda u: u.split('.')[0] if u else ""

@hookimpl
def register_routes():
    return [
        (r"^/-/instruments$", render_instrument_listing),
        (r"^/-/measures$", render_measure_listing),
        (r"^/-/instruments/(?P<instrument_name>.*)$", render_instrument),
        (r"^/-/measures/(?P<measure_name>.*)$", render_measure),
    ]

async def render_instrument(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "instrument.html.j2", {
                "instrument": datasette._doit.query_instrument(request.url_vars.get("instrument_name"))
            }, request=request
        )
    )

async def render_measure(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "measure.html.j2", {
                "measure": datasette._doit.query_measure(request.url_vars.get("measure_name"))
            }, request=request
        )
    )

async def render_instrument_listing(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "instrument_listing.html.j2", {
                "listing": datasette._doit.query_instrumentlisting()
            }, request=request
        )
    )

async def render_measure_listing(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "measure_listing.html.j2", {
                "listing": datasette._doit.query_measurelisting()
            }, request=request
        )
    )

@cache
def get_codemap(column, table, database, datasette):
    try:
        pass
#        info = datasette._doit.query_column_info(column).content
#        if isinstance(info, OrdinalMeasureItem) or isinstance(info, IndexColumn):
#            return info.codemap.value_to_text_map()
#        elif isinstance(info, SimpleMeasureItem) and info.type == 'bool':
#            return { 0: 'false', 1: 'true' }
#        else:
#            return {}
    except:
        return {}

@hookimpl
def render_cell(value, column, table, database, datasette):
    codemap = get_codemap(column, table, database, datasette)
    if codemap and value is not None:
        return markupsafe.Markup("{} <em>{}</em>".format(codemap.get(value), value))
    else:
        return None

@hookimpl
def extra_css_urls():
    return [
        "/-/static-plugins/doit/jquery.floatingscroll.css",
        "https://unpkg.com/purecss@2.1.0/build/pure-min.css",
        "/-/static-plugins/doit/doit.css",
    ]

@hookimpl
def extra_js_urls():
    return [
        "https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js",
        "/-/static-plugins/doit/jquery.floatingscroll.min.js",
    ]

@hookimpl
def get_metadata(datasette, key, database, table):
    return {
        "databases": {
            database: {
                "tables": {
                    "__codemaps__": { "hidden": True },
                    "__column_entries__": { "hidden": True },
                    "__instrument_nodes__": { "hidden": True },
                    "__instrument_entries__": { "hidden": True },
                    "__measure_entries__": { "hidden": True },
                    "__table_column_association__": { "hidden": True },
                    "__table_info__": { "hidden": True },
                }
            }
        }
    }

@hookimpl
def startup(datasette):
    async def inner():
        study_spec = app.load_study_spec(
            defaults.config_file,
            defaults.instrument_dir,
            defaults.measure_dir,
        )

        repo = SqlAlchemyRepo.new(study_spec, "")

        datasette._doit = repo
    return inner

@hookimpl
def register_commands(cli):
    @cli.command()
    def doit_preview():
        """Preview a doit configuration"""
        from datasette.app import Datasette
        study_spec = app.load_study_spec(
            defaults.config_file,
            defaults.instrument_dir,
            defaults.measure_dir,
        )

        repo = SqlAlchemyRepo.new(study_spec, "")

        ds = Datasette()

        ds._doit = repo
        
        uvicorn.run(ds.app()) # Note -- doesn't run startup hook
