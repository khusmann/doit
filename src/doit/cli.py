import typing as t

from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm

from . import app

def progress_callback(**args: t.Any):
    pbar: tqdm[int] = tqdm(total=100, unit="pct", **args)
    def update_fcn(n: int):
        pbar.n = n
        pbar.update(0)
        if n == 100:
            pbar.close()
    return update_fcn

#@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
@click.group()
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.group(name="source")
def source_cli():
    """Manage sources"""
    pass

@source_cli.command(name="add")
@click.argument('instrument_name')
@click.argument('uri')
def source_add(instrument_name: str, uri: str):
    """Add an instrument source"""
    app.add_instrument(instrument_name, uri)

@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: str):
    """Remove an instrument source"""
    pass


def complete_instrument_name(ctx: click.Context, param: click.Parameter, incomplete: str):
    pass

@source_cli.command(name="list")
@click.argument('remote_service', required=False)
def source_list(remote_service: str | None):
    """List available instruments"""
    click.secho()
    if remote_service:
        match remote_service:
            case "qualtrics":
                from .remote.qualtrics import fetch_qualtrics_listing
                for uri, title in fetch_qualtrics_listing():
                    click.secho(" {} : {}".format(click.style(uri, fg='bright_cyan'), title))
            case _:
                click.secho("Unrecognized service: {}".format(remote_service))
    else:
        click.secho("Source tables currently in workspace:")
        click.secho("TODO: Implement")
    click.secho()


@cli.group(name="sanitizer")
def sanitizer_cli():
    """Manage sanitizers"""
    pass

@sanitizer_cli.command(name="list")
@click.argument('instrument_str', required=False, shell_complete=complete_instrument_name)
def sanitizer_list(instrument_str: str | None):
    """List active sanitizers"""
    pass

@sanitizer_cli.command(name="add")
def sanitizer_add():
    """Create a new sanitizer"""
    pass

@sanitizer_cli.command(name="update")
@click.argument('instrument_str', required=False, shell_complete=complete_instrument_name)
def sanitizer_update(instrument_str: str | None):
    """Update sanitizers with new data"""
    pass

@cli.group(name="run")
def run_cli():
    """Run a processing command"""
    pass

@run_cli.command()
@click.argument('instrument_id', required=False, shell_complete=complete_instrument_name)
def fetch(instrument_id: str | None):
    """Fetch data from sources"""
    pass

@run_cli.command()
def sanitize():
    """Sanitize sources"""
    pass

@run_cli.command()
def link():
    """Link study"""
    pass

@cli.command()
def debug():
    """Debug"""
    print("Do debug stuffasdf")