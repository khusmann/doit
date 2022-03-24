from .manager.studyspec import StudySpecManager
from dotenv import load_dotenv
load_dotenv('.env')

import click
from tqdm import tqdm
import yaml
from pathlib import Path
from .manager.unsafetable import UnsafeTableManager
from .manager.safetable import SafeTableRepoManager
from .remote import fetch_table_listing
from .domain.value import InstrumentId, RemoteService, ColumnId, MeasureId
from .domain.service import sanitize_table #, stub_instrument
from .repo.study import StudyRepoReader, StudyRepoWriter

#@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
@click.group()
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.command()
def sanitize():
    """Sanitize sources"""
    unsafe_repo = UnsafeTableManager()
    safe_repo = SafeTableRepoManager()
    db_writer = safe_repo.load_writer()

    for instrument_id in tqdm(unsafe_repo.tables()):
        unsafe_table = unsafe_repo.load_table(instrument_id)
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
    unsafe_repo = UnsafeTableManager()
    unsafe_repo.add(instrument_id, uri)

@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: InstrumentId):
    """Remove an instrument source"""
    unsafe_repo = UnsafeTableManager()
    unsafe_repo.rm(instrument_id)

@cli.command(name='stub-instrument')
@click.argument('instrument_id')
def cli_stub_instrument(instrument_id: InstrumentId):
    pass
    #safe_repo = SafeTableDbRepo()
    #safe_reader = safe_repo.query()
    #safe_table = safe_reader.query(instrument_id)

    #study_repo = StudySpecManager()
    #study_repo.save_instrument(stub_instrument(safe_table))

@cli.command()
@click.argument('instrument_id')
@click.argument('column_id')
def list_unique(instrument_id: InstrumentId, column_id: ColumnId):
    safe_repo = SafeTableRepoManager()
    safe_reader = safe_repo.load_reader()
    safe_table = safe_reader.query(instrument_id)
    safe_column = safe_table.columns[column_id]
    print(yaml.dump(list(set(safe_column.values))))

@cli.command()
def debug():
    """Debug"""
    #studydb = StudyRepoReader(Path("./build/test.db"))
    #print(studydb.query_instrument_listing())
    #print(studydb.query_instrument(InstrumentId('student_behavior-y2w1')))
    #print(studydb.query_measure_listing())
    #print(studydb.query_measure(MeasureId('ssis')))

    studydb = StudyRepoWriter(Path("./build/test.db"))
    study_repo = StudySpecManager()
    study = study_repo.load_study_spec()
    studydb.setup(study)

    
@source_cli.command(name="fetch")
@click.argument('instrument_id', required=False)
def source_fetch(instrument_id: InstrumentId | None):
    unsafe_repo = UnsafeTableManager()
    id_list = unsafe_repo.tables() if instrument_id is None else [instrument_id]
    for i in id_list:
        unsafe_repo.fetch(i)

@source_cli.command(name="list")
@click.argument('remote_service', required=False)
def source_list(remote_service: RemoteService | None):
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
