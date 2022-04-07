from .manager.studyspec import StudySpecManager
from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm
import yaml
from .manager.unsafetable import UnsafeTableManager
from .manager.sourcetable import SourceTableRepoManager
from .manager.study import StudyRepoManager
from .remote import fetch_table_listing

from .domain.service import *

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

@cli.group('source')
def source_cli():
    """Manipulate instrument sources"""
    pass

@source_cli.command(name="add")
@click.argument('instrument_id')
@click.argument('uri')
def source_add(instrument_id: InstrumentName, uri: str):
    """Add an instrument source"""
    unsafe_repo = UnsafeTableManager()
    unsafe_repo.add(instrument_id, uri)

@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: InstrumentName):
    """Remove an instrument source"""
    unsafe_repo = UnsafeTableManager()
    unsafe_repo.rm(instrument_id)

@cli.command(name='stub-instrument')
@click.argument('instrument_id')
def cli_stub_instrument(instrument_id: InstrumentName):
    pass
    #safe_repo = SafeTableDbRepo()
    #safe_reader = safe_repo.query()
    #safe_table = safe_reader.query(instrument_id)

    #study_repo = StudySpecManager()
    #study_repo.save_instrument(stub_instrument(safe_table))

@cli.command()
@click.argument('instrument_id')
@click.argument('column_id')
def list_unique(instrument_id: InstrumentName, column_id: SourceColumnName):
    safe_repo = SourceTableRepoManager()
    safe_reader = safe_repo.load_repo_readonly()
    safe_table = safe_reader.query(instrument_id)
    safe_column = safe_table.columns[column_id]
    print(yaml.dump(list(set(safe_column.values))))

@cli.command()
def debug():
    """Debug"""
    study_repo = StudyRepoManager().load_repo()
    study_spec = StudySpecManager().load_study_spec()

    study_repo = study_repo.mutate(mutations_from_study_spec(study_spec))
    study_repo = study_repo.create_tables()

    source_table_repo = SourceTableRepoManager().load_repo_readonly()

    instruments = study_repo.query_instruments()
    for i in tqdm(instruments):
        source_table = source_table_repo.query(i.name)
        study_repo.add_source_data(link_source_table(i, source_table))

    
@source_cli.command(name="fetch")
@click.argument('instrument_id', required=False)
def source_fetch(instrument_id: InstrumentName | None):
    unsafe_repo = UnsafeTableManager()
    id_list = unsafe_repo.tables() if instrument_id is None else [instrument_id]
    for i in id_list:
        unsafe_repo.fetch(i)

@source_cli.command(name="list")
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
