import typing as t

from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm

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

@cli.command(name="add")
@click.argument('instrument_id')
@click.argument('uri')
def source_add(instrument_id: str, uri: str):
    """Add an instrument source"""
    from .manager import UnsafeTableManager, StudySpecManager
    from .domain.service import stub_instrument_spec
    from .domain.value import InstrumentName

    instrument_id = InstrumentName(instrument_id)

    unsafe_manager = UnsafeTableManager()
    study_spec = StudySpecManager()
    unsafe_manager.add(instrument_id, uri)

    click.secho()
    click.secho("Fetching instrument: {}".format(instrument_id))
    update_fcn = progress_callback()
    table = unsafe_manager.fetch(instrument_id, update_fcn)

    click.secho()
    click.secho("Stubbing instrument file: {}".format(study_spec.settings.instrument_file(instrument_id)))
    if instrument_id in study_spec.instruments:
        click.secho()
        click.secho("Warning: {} instrument file already exists; saving backup...".format(instrument_id), fg='bright_red')
    
    instrument_spec = stub_instrument_spec(table)
    study_spec.save_instrument_spec(instrument_id, instrument_spec)
    click.secho()
    click.secho("Done.")
    click.secho()

@cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: str):
    """Remove an instrument source"""
    from .manager import UnsafeTableManager
    from .domain.value import InstrumentName

    unsafe_repo = UnsafeTableManager()
    unsafe_repo.rm(InstrumentName(instrument_id))


def complete_instrument_name(ctx: click.Context, param: click.Parameter, incomplete: str):
    from .settings import ProjectSettings
    return [i for i in ProjectSettings().get_unsafe_table_names() if i.startswith(incomplete)]

@cli.command()
@click.argument('instrument_id', required=False, shell_complete=complete_instrument_name)
def fetch(instrument_id: str | None):
    from .manager import UnsafeTableManager
    from .domain.value import InstrumentName

    unsafe_repo = UnsafeTableManager()
    id_list = unsafe_repo.tables() if instrument_id is None else [InstrumentName(instrument_id)]
    click.secho()
    for i in tqdm(id_list):
        update_fcn = progress_callback(leave=False, desc=i)
        unsafe_repo.fetch(i, update_fcn)
    click.secho()

@cli.command(name="list")
@click.argument('remote_service', required=False)
def source_list(remote_service: str | None):
    """List available instruments"""
    from .manager import UnsafeTableManager
    from .remote import fetch_table_listing
    from .domain.value import RemoteServiceName


    unsafe_repo = UnsafeTableManager()

    click.secho()
    if (remote_service is None):
        for (instrument_id, file_info) in map(lambda i: (i, unsafe_repo.load_file_info(i)), unsafe_repo.tables()):
            click.secho(" {} : {}".format(click.style(instrument_id, fg='bright_cyan'), file_info.remote.uri))
    else:
        for desc in fetch_table_listing(RemoteServiceName(remote_service)):
            click.secho(" {} : {}".format(click.style(desc.uri, fg='bright_cyan'), desc.title))
    click.secho()

@cli.command()
def sanitize():
    """Sanitize sources"""
    from .manager import UnsafeTableManager, SourceTableRepoManager
    from .domain.service import sanitize_table

    unsafe_manager = UnsafeTableManager()
    safe_repo = SourceTableRepoManager().load_repo()

    click.secho()
    for instrument_id in tqdm(unsafe_manager.tables()):
        unsafe_table = unsafe_manager.load_unsafe_table(instrument_id)
        safe_table = sanitize_table(unsafe_table)
        safe_repo.insert(safe_table)
    click.secho()

@cli.command()
def link():
    """Link study"""
    from .manager import StudyRepoManager, StudySpecManager, SourceTableRepoManager
    from .domain.service import mutations_from_study_spec, link_source_table

    study_repo = StudyRepoManager().load_repo()
    study_spec = StudySpecManager().load_study_spec()

    study_repo = study_repo.mutate(mutations_from_study_spec(study_spec))
    study_repo = study_repo.create_tables()

    click.secho()

    try:
        source_table_repo = SourceTableRepoManager().load_repo_readonly()
    except:
        click.secho("Warning: Database created, but no source data to link...", fg='bright_red')
        print()
        source_table_repo = None
    
    if source_table_repo:
        instruments = study_repo.query_instruments()
        warnings: t.List[str] = []
        for i in tqdm(instruments):
            try:
                source_table = source_table_repo.query(i.name)
                study_repo.add_source_data(link_source_table(i, source_table))
            except:
                warnings += ["Warning: instrument '{}' does not exist in source data".format(i.name)]
        click.secho()
        for w in warnings:
            click.secho(w, fg='bright_red')
        click.secho()

    click.secho("Linked database created. View by running:")
    click.secho()
    click.secho("datasette {}".format(study_repo.path), fg='bright_cyan')
    click.secho()

@cli.command()
def debug():
    """Debug"""
    print("Do debug stuff")
    
