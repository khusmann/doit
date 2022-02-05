import click

from .values import Study

def complete_instruments(ctx: click.Context, param: click.Argument, incomplete: str):
    assert isinstance(ctx.obj, Study), "Error: Expected ctx.obj to be a Study"
    return [i.name for i in ctx.obj.instruments if i.name.startswith(incomplete)]

def complete_versions(ctx: click.Context, param: click.Argument, incomplete: str):
    assert isinstance(ctx.obj, Study), "Error: Expected ctx.obj to be a Study"
    return [iv.name for i in ctx.obj.instruments for iv in i.versions if i.name == ctx.params['instrument'] and iv.name.startswith(incomplete)]