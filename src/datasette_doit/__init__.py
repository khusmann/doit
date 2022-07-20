# type: ignore
import typing as t
from datasette import hookimpl
from pathlib import Path
from datasette.utils.asgi import Response
from functools import cache
import markupsafe
import click
import html
import json

import uvicorn
from doit import app, settings
from doit.study.sqlalchemy.impl import SqlAlchemyRepo
from doit.study.view import CodedColumnView, IndexColumnView

defaults = settings.AppSettings()



@hookimpl
def menu_links(datasette, actor):
    return [
#       {"href": datasette.urls.path("/-/instruments"), "label": "Instruments"},
    ]

@hookimpl
def prepare_jinja2_environment(env):
    env.filters['base_id'] = lambda u: u.split('.')[0] if u else ""

@hookimpl
def register_routes():
    return [
        (r"/(?P<as_format>(\.jsono?)?$)", render_index),
        (r"^/-/instruments/(?P<instrument_name>.*)$", render_instrument),
        (r"^/-/measures/(?P<measure_name>.*)$", render_measure),
    ]

async def render_index(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "index.html.j2", {
                "instruments": datasette._doit.query_instrumentlisting(),
                "measures": datasette._doit.query_measurelisting(),
                "title": datasette._doit_title,
                "database": list(datasette.databases.keys())[1],
            }, request=request
        )
    )

async def render_instrument(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "instrument.html.j2", {
                "instrument": datasette._doit.query_instrument(request.url_vars.get("instrument_name")),
                "database": list(datasette.databases.keys())[1]
            }, request=request
        )
    )

async def render_measure(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "measure.html.j2", {
                "measure": datasette._doit.query_measure(request.url_vars.get("measure_name")),
                "database": list(datasette.databases.keys())[1]
            }, request=request
        )
    )

@cache
def get_codemap(column, table, database, datasette) -> t.Mapping[int, str]:
    try:
        info = datasette._doit.query_column(column)
        if isinstance(info, CodedColumnView | IndexColumnView):
            return info.codes.tag_from_value
        else:
            return {}
    except:
        return {}

@cache
def remap_value(codemap: t.Sequence[t.Tuple[int, str]], value):
    codemap=dict(codemap)
    if isinstance(value, int):
        return codemap.get(value)
    else:
        return "[{}]".format(", ".join(codemap.get(i) for i in json.loads(value)))
    

@hookimpl
def render_cell(value, column, table, database, datasette):
    codemap = get_codemap(column, table, database, datasette)
    if codemap and value is not None:
        result = remap_value(tuple(codemap.items()), value)
        return markupsafe.Markup("{} <em>{}</em>".format(result, value))
    else:
        return None

@hookimpl
def extra_css_urls():
    return [
        "/-/static-plugins/doit/jquery.floatingscroll.css",
        "https://unpkg.com/purecss@2.1.0/build/pure-min.css",
        "/-/static-plugins/doit/doit.css",
        "//cdn.datatables.net/1.12.1/css/jquery.dataTables.min.css",
        "https://code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css",
    ]

@hookimpl
def extra_js_urls():
    return [
        "https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js",
        "/-/static-plugins/doit/jquery.floatingscroll.min.js",
        "//cdn.datatables.net/1.12.1/js/jquery.dataTables.min.js",
        "//code.jquery.com/ui/1.12.1/jquery-ui.js",
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
                    "__column_entry_dependencies__": { "hidden": True },
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
            defaults.package_dir,
        )

        repo = SqlAlchemyRepo.new(study_spec, "")

        datasette._doit = repo
        datasette._doit_title = study_spec.config.title
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
            defaults.package_dir,
        )

        repo = SqlAlchemyRepo.new(study_spec, "")

        ds = Datasette()

        ds._doit = repo
        ds._doit_title = study_spec.config.title
        
        uvicorn.run(ds.app(), port=8001) # Note -- doesn't run startup hook


@hookimpl
def extra_body_script():
    return {
        "module": False,
        "script": "$(document).ready(function () {$('.table-wrapper').floatingScroll();});",
    }