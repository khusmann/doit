from .repo.study import StudyRepo
from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm
import yaml

from .repo.unsafetable import UnsafeTableRepo
from .repo.safetabledb import SafeTableDbRepo
from .io.remote import fetch_table_listing
from .domain.value import InstrumentId, RemoteService, ColumnId
from .domain.service import sanitize_table, stub_instrument

#@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
@click.group()
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.command()
def sanitize():
    """Sanitize sources"""
    unsafe_repo = UnsafeTableRepo()
    safe_repo = SafeTableDbRepo()
    db_writer = safe_repo.new_db()

    for instrument_id in tqdm(unsafe_repo.tables()):
        unsafe_table = unsafe_repo.query(instrument_id)
        safe_table = sanitize_table(unsafe_table)
        db_writer.insert(safe_table)

@cli.group('source')
def source_cli():
    """Manipulate instrument sources"""
    pass

@source_cli.command(name="add")
@click.argument('instrument_id')
@click.argument('uri')
def source_add(instrument_id: InstrumentId, uri: str):
    """Add an instrument source"""
    unsafe_repo = UnsafeTableRepo()
    unsafe_repo.add(instrument_id, uri)

@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: InstrumentId):
    """Remove an instrument source"""
    unsafe_repo = UnsafeTableRepo()
    unsafe_repo.rm(instrument_id)

@cli.command(name='stub-instrument')
@click.argument('instrument_id')
def cli_stub_instrument(instrument_id: InstrumentId):
    safe_repo = SafeTableDbRepo()
    safe_reader = safe_repo.query()
    safe_table = safe_reader.query(instrument_id)

    study_repo = StudyRepo()
    study_repo.save_instrument(stub_instrument(safe_table))

@cli.command()
@click.argument('instrument_id')
@click.argument('column_id')
def list_unique(instrument_id: InstrumentId, column_id: ColumnId):
    safe_repo = SafeTableDbRepo()
    safe_reader = safe_repo.query()
    safe_table = safe_reader.query(instrument_id)
    safe_column = safe_table.columns[column_id]
    print(yaml.dump(list(set(safe_column.values))))

@cli.command()
def debug():
    """Debug"""
    study_repo = StudyRepo()

#    print(study_repo.query().measure_items)
    print(study_repo.query().tables)

    #safe_repo = SafeTableDbRepo()
    #db_reader = safe_repo.query()
    #print(db_reader.query(InstrumentId('student_behavior-y2w2')))

    #unsafe_repo = UnsafeTableRepo()
    #print(unsafe_repo.query(instrument_id).json(indent=2))
    
@source_cli.command(name="fetch")
@click.argument('instrument_id', required=False)
def source_fetch(instrument_id: InstrumentId | None):
    unsafe_repo = UnsafeTableRepo()
    id_list = unsafe_repo.tables() if instrument_id is None else [instrument_id]
    for i in id_list:
        unsafe_repo.fetch(i)

@source_cli.command(name="list")
@click.argument('remote_service', required=False)
def source_list(remote_service: RemoteService | None):
    """List available instruments"""
    unsafe_repo = UnsafeTableRepo()
    click.secho()
    if (remote_service is None):
        for meta in map(lambda i: unsafe_repo.query_meta(i), unsafe_repo.tables()):
            click.secho(" {} : {}".format(click.style(meta.instrument_id, fg='bright_cyan'), meta.file_info.remote.uri))
    else:
        for desc in fetch_table_listing(remote_service):
            click.secho(" {} : {}".format(click.style(desc.uri, fg='bright_cyan'), desc.title))
    click.secho()
