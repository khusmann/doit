from dotenv import load_dotenv
load_dotenv('.env')

import click

from .repo.unsafetable import UnsafeTableRepo
from .io.remote import fetch_table_listing

#@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
@click.group()
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.group('source')
def source_cli():
    """Manipulate instrument sources"""
    pass

@source_cli.command(name="add")
@click.argument('instrument_id')
@click.argument('uri')
def source_add(instrument_id: str, uri: str):
    """Add an instrument source"""
    repo = UnsafeTableRepo()
    repo.add(instrument_id, uri)

@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: str):
    """Remove an instrument source"""
    repo = UnsafeTableRepo()
    repo.rm(instrument_id)

@source_cli.command(name="load")
@click.argument('instrument_id')
def source_load(instrument_id: str):
    """Remove an instrument source"""
    repo = UnsafeTableRepo()
    print(repo.query(instrument_id))
    
@source_cli.command(name="fetch")
@click.argument('instrument_id', required=False)
def source_fetch(instrument_id: str | None):
    repo = UnsafeTableRepo()
    id_list = repo.tables() if instrument_id is None else [instrument_id]
    for i in id_list:
        repo.fetch(i)

@source_cli.command(name="list")
@click.argument('remote', required=False)
def source_list(remote: str | None):
    """List available instruments"""
    repo = UnsafeTableRepo()
    click.secho()
    if (remote is None):
        for info in map(lambda i: repo.query_source_info(i), repo.tables()):
            click.secho(" {} : {}".format(click.style(info.instrument_id, fg='bright_cyan'), info.uri))
    else:
        for desc in fetch_table_listing(remote):
            click.secho(" {} : {}".format(click.style(desc.uri, fg='bright_cyan'), desc.title))
    click.secho()
