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
    from .service.sanitize import update_tablesanitizer
    from .remote.blob import load_blob
    click.secho()
    progress = progress_callback()
    blob = app.add_source(
        instrument_name,
        uri,
        progress,
        defaults.blob_from_instrument_name,
    )

    table = load_blob(blob)

    existing_sanitizers = app.load_table_sanitizer(
        instrument_name,
        defaults.sanitizer_dir_from_instrument_name,
    )

    updates = update_tablesanitizer(table, existing_sanitizers)

    app.update_sanitizer(
        instrument_name,
        updates,
        defaults.sanitizer_dir_from_instrument_name,
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
@click.argument('instrument_name', required=False, shell_complete=complete_instrument_name)
def sanitizer_update(instrument_name: str | None):
    """Update sanitizers with new data"""
    from .service.sanitize import update_tablesanitizer
    if instrument_name:
        items = (instrument_name,)
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        items = tuple(l.name for l in listing)

    click.secho()

    for name in items:
        table = app.load_unsanitizedtable(
            name,
            defaults.blob_from_instrument_name,
        )
        table_sanitizer = app.load_table_sanitizer(
            name,
            defaults.sanitizer_dir_from_instrument_name,
        )

        updates = update_tablesanitizer(table, table_sanitizer)

        if updates:
            click.secho("Updating sanitizers for {}".format(click.style(name, fg="bright_cyan")))
            for u in updates:
                if u.new:
                    click.secho("    Creating new sanitizer {} ({}) items".format(u.name, len(u.rows)))
                else:
                    click.secho("    Updating sanitizer {} ({}) items".format(u.name, len(u.rows)))

        app.update_sanitizer(
            name,
            updates,
            defaults.sanitizer_dir_from_instrument_name
        )

    click.secho()

@cli.command(name="run")
def run_cli():
    """Run the entire pipeline (fetch, sanitize, link)"""
    pass

@cli.command()
@click.argument('instrument_name', required=False, shell_complete=complete_instrument_name)
def stub(instrument_name: str | None):
    """Stub an instrument definition"""
    from .service.link import stub_instrumentspec
    click.secho()

    if instrument_name:
        items = (instrument_name,)
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        items = tuple(l.name for l in listing)

    sanitized_repo = app.open_sanitizedtable_repo(
        defaults.sanitized_repo_path,
    )

    for name in items:
        table = sanitized_repo.read_table(name)
        if table.info.source == "qualtrics":
            from .study.io.qualtrics import stub_instrumentspec_from_qualtrics_blob
            stub = stub_instrumentspec_from_qualtrics_blob(defaults.blob_from_instrument_name(name))
        else:
            stub = stub_instrumentspec(table)

        filename = app.write_instrument_spec_stub(
            name,
            stub,
            defaults.instrument_stub_from_instrument_name,
        )
        click.secho("Wrote: {}".format(click.style(filename, fg="bright_cyan")))

    click.secho()


@cli.command()
@click.argument('instrument_name', required=False, shell_complete=complete_instrument_name)
def fetch(instrument_name: str | None):
    """Fetch data from sources"""
    from .service.sanitize import update_tablesanitizer
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

        sanitizer = app.load_table_sanitizer(
            name,
            defaults.sanitizer_dir_from_instrument_name,
        )

        updates = update_tablesanitizer(table, sanitizer)

        app.update_sanitizer(
            name,
            updates,
            defaults.sanitizer_dir_from_instrument_name,
        )

    click.secho()

@cli.command()
def sanitize():
    """Sanitize sources"""
    from .service.sanitize import sanitize_table
    from .common.table import TableErrorReport
    
    listing = app.get_local_source_listing(
        defaults.source_dir,
        defaults.blob_from_instrument_name
    )

    repo = app.new_sanitizedtable_repo(
        defaults.sanitized_repo_path,
        defaults.sanitized_repo_bkup_path,
    )
    
    errors: TableErrorReport = set()

    click.secho()
    for entry in tqdm(listing):
        unsanitized = app.load_unsanitizedtable(
            entry.name,
            defaults.blob_from_instrument_name
        )

        sanitizer = app.load_table_sanitizer(
            entry.name,
            defaults.sanitizer_dir_from_instrument_name,
        )

        sanitized = sanitize_table(unsanitized, sanitizer)
        new_errors = repo.write_table(sanitized)
        errors |= new_errors

    if errors:
        click.secho()
        click.secho("Encountered {} errors.".format(len(errors)), fg='bright_red')
        click.secho()
        click.secho("See {} for more info".format(click.style(defaults.error_file_path, fg='bright_cyan')))
        app.write_errors(errors, defaults.error_file_path)

    click.secho()

@cli.command()
def link():
    """Link study"""
    from .service.link import (
        link_tableinfo,
        link_table,
    )

    from .common.table import TableErrorReport
    
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

    errors: TableErrorReport = set()

    click.secho()

    for linker in tqdm(linkers):
        sanitized_table = sanitized_repo.read_table(linker.instrument_name)
        linked_table = link_table(sanitized_table.data, linker)
        new_errors = linked_repo.write_table(linked_table)
        errors |= new_errors

    if errors:
        click.secho()
        click.secho("Encountered {} errors.".format(len(errors)), fg='bright_red')
        click.secho()
        click.secho("See {} for more info".format(click.style(defaults.error_file_path, fg='bright_cyan')))
        app.write_errors(errors, defaults.error_file_path)
    
    click.secho()
    click.secho("View by running: {}".format(click.style("datasette {}".format(defaults.study_repo_path), fg="bright_cyan")))
    click.secho()

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