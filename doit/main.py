from .manager.studyspec import StudySpecManager
from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm
from .manager.unsafetable import UnsafeTableManager
from .manager.sourcetable import SourceTableRepoManager
from .manager.study import StudyRepoManager
from .remote import fetch_table_listing

from .domain.service import *

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

@cli.command()
def sanitize():
    """Sanitize sources"""
    unsafe_manager = UnsafeTableManager()
    safe_repo = SourceTableRepoManager().load_repo()

    for instrument_id in tqdm(unsafe_manager.tables()):
        unsafe_table = unsafe_manager.load_table(instrument_id)
        safe_table = sanitize_table(unsafe_table)
        safe_repo.insert(safe_table)

@cli.command(name="add")
@click.argument('instrument_id')
@click.argument('uri')
def source_add(instrument_id: InstrumentName, uri: str):
    """Add an instrument source"""
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
def source_rm(instrument_id: InstrumentName):
    """Remove an instrument source"""
    unsafe_repo = UnsafeTableManager()
    unsafe_repo.rm(instrument_id)

@cli.command()
@click.argument('instrument_id', required=False)
def fetch(instrument_id: InstrumentName | None):
    unsafe_repo = UnsafeTableManager()
    id_list = unsafe_repo.tables() if instrument_id is None else [instrument_id]
    click.secho()
    for i in tqdm(id_list):
        update_fcn = progress_callback(leave=False, desc=i)
        unsafe_repo.fetch(i, update_fcn)
    click.secho()

@cli.command(name="list")
@click.argument('remote_service', required=False)
def source_list(remote_service: RemoteServiceName | None):
    """List available instruments"""
    unsafe_repo = UnsafeTableManager()
    click.secho()
    if (remote_service is None):
        for (instrument_id, file_info) in map(lambda i: (i, unsafe_repo.load_file_info(i)), unsafe_repo.tables()):
            click.secho(" {} : {}".format(click.style(instrument_id, fg='bright_cyan'), file_info.remote.uri))
    else:
        for desc in fetch_table_listing(remote_service):
            click.secho(" {} : {}".format(click.style(desc.uri, fg='bright_cyan'), desc.title))
    click.secho()

@cli.command()
def link():
    """Link study"""
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
    
