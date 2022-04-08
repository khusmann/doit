# type: ignore
from datasette import hookimpl
from ..manager.study import StudyRepoManager
from pathlib import Path
from datasette.utils.asgi import Response
import html

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
                "instruments": datasette._doit.query_instruments()
            }, request=request
        )
    )

async def render_measure_listing(scope, receive, datasette, request):
    return Response.html(
        await datasette.render_template(
            "measure_listing.html.j2", {
                "measures": datasette._doit.query_measures()
            }, request=request
        )
    )

@hookimpl
def render_cell(value, column, table, database, datasette):
    pass

@hookimpl
def extra_css_urls():
    return [
        "/-/static-plugins/doit/jquery.floatingscroll.css"
    ]

@hookimpl
def extra_js_urls():
    return [
        "https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js",
        "/-/static-plugins/doit/jquery.floatingscroll.min.js",
    ]


@hookimpl
def startup(datasette):
    async def inner():
        datasette._doit = StudyRepoManager().load_repo_readonly()
    return inner