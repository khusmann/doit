from dotenv import load_dotenv
load_dotenv('.env')

import click

from . import io

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
    io.InstrumentSource(instrument_id=instrument_id, uri=uri).save()


@source_cli.command(name="rm")
@click.argument('instrument_id')
def source_rm(instrument_id: str):
    """Remove an instrument source"""
    io.InstrumentSource.load(instrument_id).rm()

@source_cli.command(name="load")
@click.argument('instrument_id')
def source_load(instrument_id: str):
    """Remove an instrument source"""
    print(io.InstrumentSource.load(instrument_id).load_data())


@source_cli.command(name="fetch")
@click.argument('instrument_id', required=False)
def source_fetch(instrument_id: str | None):
    instrument_list = (io.InstrumentSource.load_all().values() if instrument_id is None
                       else [io.InstrumentSource.load(instrument_id)])
    for i in instrument_list:
        i.fetch()

@source_cli.command(name="list")
@click.argument('remote', required=False)
def source_list(remote: str | None):
    """List available instruments"""
    click.secho()
    if (remote is None):
        for (instrument_id, source) in io.InstrumentSource.load_all().items():
            click.secho(" {} : {}".format(click.style(instrument_id, fg='bright_cyan'), source.uri))
    else:
        for desc in io.fetch_remote_instrument_desc(remote):
            click.secho(" {} : {}".format(click.style(desc.uri, fg='bright_cyan'), desc.title))
    click.secho()
