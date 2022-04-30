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
    from .service.sanitize import update_tablesanitizers
    click.secho()

    if instrument_name:
        items = (instrument_name,)
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        items = tuple(l.name for l in listing)

    for name in tqdm(items):
        progress = progress_callback(leave=False, desc=name)

        table = app.fetch_source(
            name,
            progress,
            defaults.blob_from_instrument_name,
            defaults.blob_bkup_filename,
        )

        sanitizers = app.load_sanitizers(
            name,
            defaults.sanitizer_dir_from_instrument_name,
        )

        updates = update_tablesanitizers(table, sanitizers)

        app.update_sanitizers(
            name,
            updates,
            defaults.sanitizer_dir_from_instrument_name,
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

    repo = app.new_sanitizedtable_repo(
        defaults.sanitized_repo_path,
        defaults.sanitized_repo_bkup_path,
    )

    click.secho()
    for entry in tqdm(listing):
        unsanitized = app.load_unsanitizedtable(
            entry.name,
            defaults.blob_from_instrument_name
        )

        sanitizers = app.load_sanitizers(
            entry.name,
            defaults.sanitizer_dir_from_instrument_name,
        )

        sanitized = sanitize_table(unsanitized, tuple(sanitizers.values()))
        repo.write_table(sanitized, entry.name)
    click.secho()

@cli.command()
def link():
    """Link study"""
    from .service.link import link_tableinfo, link_tabledata
    
    sanitized_repo = app.open_sanitizedtable_repo(
        defaults.sanitized_repo_path,
    )

    study_spec = app.load_study_spec(
        defaults.config_file,
        defaults.instrument_dir,
        defaults.measure_dir,
    )

    linked_repo = app.new_study_repo(
        study_spec,
        defaults.study_repo_path,
        defaults.study_repo_bkup_path,
    )

    instrumentlinker_specs = linked_repo.query_instrumentlinkerspecs()

    sanitizedtableinfos = tuple(
        sanitized_repo.read_tableinfo(i.instrument_name) for i in instrumentlinker_specs
    ) 

    linkers = tuple(
        link_tableinfo(info, spec) for info, spec in zip(sanitizedtableinfos, instrumentlinker_specs)
    )

    for linker in tqdm(linkers):
        sanitized_table = sanitized_repo.read_table(linker.instrument_name)
        linked_table = link_tabledata(sanitized_table.data, linker)
        print(linked_table.rows)

    pass

@cli.command()
def debug():
    """Debug"""
    print("Do debug stuffasdf")

    table = app.load_study_spec(
        defaults.config_file,
        defaults.instrument_dir,
        defaults.measure_dir,
    )

    print(table)