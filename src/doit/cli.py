import typing as t

from dotenv import load_dotenv
from doit.sanitizer.model import LookupSanitizer

from doit.study.repo import StudyRepoReader

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
    from .service.sanitize import update_studysanitizers
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

    existing_sanitizers = app.load_study_sanitizers(
        defaults.sanitizer_repo_filename,
    )

    updated_sanitizers = update_studysanitizers(instrument_name, table, existing_sanitizers)

    app.update_sanitizer(
        updated_sanitizers,
        defaults.sanitizer_repo_filename,
        defaults.sanitized_repo_bkup_path,
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
@click.option('-a', '--all', "list_all", is_flag=True)
def source_list(remote_service: str | None, list_all: bool):
    """List available instruments"""
    click.secho()
    if remote_service:
        local_items = { uri for _, _, uri in app.get_local_source_listing(defaults.source_dir, defaults.blob_from_instrument_name) }
        for uri, title in app.get_remote_source_listing(remote_service):
            if uri not in local_items or list_all:
                click.secho(" {} : {}".format(click.style(uri, fg='bright_cyan'), title))
    else:
        for name, title, _ in sorted(app.get_local_source_listing(defaults.source_dir, defaults.blob_from_instrument_name), key=lambda x: x.name):
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
@click.argument('instrument_name')
@click.argument('src_ids')
@click.argument('dst_ids', required=False)
def sanitizer_add(instrument_name: str, src_ids: str, dst_ids: str | None):
    """Create a new sanitizer"""
    from .unsanitizedtable.model import UnsanitizedColumnId
    from .sanitizedtable.model import SanitizedColumnId
    from .service.sanitize import update_lookupsanitizer
    from .sanitizer.io import sanitizer_tospec

    if not dst_ids:
        dst_ids = src_ids

    table = app.load_unsanitizedtable(
        instrument_name,
        defaults.blob_from_instrument_name,
    )

    sanitizer = LookupSanitizer(
        key_col_ids=tuple(UnsanitizedColumnId(i) for i in src_ids.split()),
        new_col_ids=tuple(SanitizedColumnId(i) for i in dst_ids.split()),
        prompt=",".join((i.prompt for i in table.schema if i.id.unsafe_name in src_ids.split())),
        map={},
    )

    study_sanitizer = update_lookupsanitizer(table, sanitizer)

    import yaml

    def ordered_dict_dumper(dumper: yaml.Dumper, data: t.Dict[t.Any, t.Any]):
        return dumper.represent_dict(data.items())

    def tuple_dumper(dumper: yaml.Dumper, tuple: t.Tuple[t.Any, ...]):
        return dumper.represent_list(tuple)

    yaml.add_representer(dict, ordered_dict_dumper)
    yaml.add_representer(tuple, tuple_dumper)

    print(yaml.dump(sanitizer_tospec(study_sanitizer).dict()))




@sanitizer_cli.command(name="update")
@click.argument('instrument_name', required=False, shell_complete=complete_instrument_name)
def sanitizer_update(instrument_name: str | None):
    """Update sanitizers with new data"""
    from .service.sanitize import update_studysanitizers
    if instrument_name:
        items = (instrument_name,)
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        items = tuple(l.name for l in listing)

    click.secho()

    study_sanitizer = app.load_study_sanitizers(
        defaults.sanitizer_repo_filename,
    )

    for name in items:
        table = app.load_unsanitizedtable(
            name,
            defaults.blob_from_instrument_name,
        )
        study_sanitizer = update_studysanitizers(name, table, study_sanitizer)

    app.update_sanitizer(
        study_sanitizer,
        defaults.sanitizer_repo_filename,
        defaults.sanitizer_repo_bkup_path,
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
            click.secho("{}...".format(name))
            stub = stub_instrumentspec_from_qualtrics_blob(defaults.blob_from_instrument_name(name))
        else:
            stub = stub_instrumentspec(table)

        if defaults.instrument_stub_from_instrument_name(name).exists():
            click.secho("Already exists: {}".format(click.style(name, fg="bright_cyan")))
        else:
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
    from .service.sanitize import update_studysanitizers
    click.secho()

    if instrument_name:
        items = (instrument_name,)
    else:
        listing = app.get_local_source_listing(
            defaults.source_dir,
            defaults.blob_from_instrument_name
        )
        items = tuple(l.name for l in listing)

    study_sanitizers = app.load_study_sanitizers(
        defaults.sanitizer_repo_filename,
    )

    for name in tqdm(items):
        progress = progress_callback(leave=False, desc=name)

        table = app.fetch_source(
            name,
            progress,
            defaults.blob_from_instrument_name,
            defaults.blob_bkup_filename,
        )

        study_sanitizers = update_studysanitizers(name, table, study_sanitizers)


    app.update_sanitizer(
        study_sanitizers,
        defaults.sanitizer_repo_filename,
        defaults.sanitized_repo_bkup_path,
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

    study_sanitizers = app.load_study_sanitizers(
        defaults.sanitizer_repo_filename,
    )

    click.secho()
    for entry in tqdm(listing):
        unsanitized = app.load_unsanitizedtable(
            entry.name,
            defaults.blob_from_instrument_name
        )

        sanitized = sanitize_table(unsanitized, study_sanitizers.table_sanitizers[entry.name])
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
        defaults.package_dir,
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

    from .package import package_data
    assert(isinstance(linked_repo, StudyRepoReader))
    for p in study_spec.packages.values():
        package_data(p, linked_repo, defaults.package_repo_dir)

@cli.command()
def debug():
    """Debug"""
    print("Do debug stuffasdf")

    table = app.load_study_spec(
        defaults.config_file,
        defaults.instrument_dir,
        defaults.measure_dir,
        defaults.package_dir,
    )

    print(table)