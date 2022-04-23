import typing as t

from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm

from . import app
from .settings import AppSettings

defaults = AppSettings()

def progress_callback(**args: t.Any):
    pbar: tqdm[int] = tqdm(total=100, unit="pct", **args)
    def update_fcn(n: int):
        pbar.n = n
        pbar.update(0)
        if n >= 100:
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
    click.secho()
    progress = progress_callback()
    app.add_source(
        instrument_name,
        uri,
        progress,
        defaults.blob_from_instrument_name,
    )
    click.secho()

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
        for uri, title in app.get_remote_source_listing(remote_service):
            click.secho(" {} : {}".format(click.style(uri, fg='bright_cyan'), title))
    else:
        for name, title in app.get_local_source_listing(defaults.source_dir, defaults.blob_from_instrument_name):
            click.secho(" {} : {}".format(click.style(name, fg='bright_cyan'), title))
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

@cli.command(name="run")
def run_cli():
    """Run the entire pipeline (fetch, sanitize, link)"""
    pass

@cli.command()
@click.argument('instrument_name', required=False, shell_complete=complete_instrument_name)
def fetch(instrument_name: str | None):
    """Fetch data from sources"""
    click.secho()
    if instrument_name:
        progress = progress_callback()
        app.fetch_source(
            instrument_name,
            progress,
            defaults.blob_from_instrument_name,
            defaults.blob_bkup_filename,
        )
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        for name, title in tqdm(listing):
            progress = progress_callback(leave=False, desc=title)
            app.fetch_source(
                name,
                progress,
                defaults.blob_from_instrument_name,
                defaults.blob_bkup_filename,
            )
    click.secho()

@cli.command()
def sanitize():
    """Sanitize sources"""
    from .service.sanitize import sanitize_table
    
    listing = app.get_local_source_listing(
        defaults.source_dir,
        defaults.blob_from_instrument_name
    )

    repo = app.get_sanitizedtable_repo(
        defaults.sanitized_repo_path,
    )

    click.secho()
    for entry in tqdm(listing):
        unsanitized = app.load_unsanitizedtable(
            entry.name,
            defaults.blob_from_instrument_name
        )
        # TODO: Load sanitizers
        sanitized = sanitize_table(unsanitized, [])
        repo.write_table(sanitized, entry.name)
    click.secho()

@cli.command()
def link():
    """Link study"""
    pass

@cli.command()
def debug():
    """Debug"""
    print("Do debug stuffasdf")

    table = app.load_unsanitizedtable('test_survey', defaults.blob_from_instrument_name)

    print(table)