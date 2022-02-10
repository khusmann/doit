from dotenv import load_dotenv
load_dotenv('.env')

import click

from .config import load_defaults
from .utils import complete_instruments, complete_versions
from .context import load_study_context
from . import io

from .domain import helpers

@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.group('qualtrics')
def qualtrics_cli():
    """Manipulate Qualtrics surveys"""
    pass

@qualtrics_cli.command(name="schema")
@click.argument('id')
def qualtrics_schema(id: str):
    """Add a Qualtrics survey"""
    io.save_instrument_config(helpers.new_instrument_config("student_behavior", "y1w1", io.load_schema("qualtrics://{}".format(id))))

@qualtrics_cli.command(name="add")
@click.argument('id')
def qualtrics_add(id: str):
    """Add a Qualtrics survey"""
    io.download_blob("qualtrics://{}".format(id))

@qualtrics_cli.command(name="list")
def qualtrics_list():
    """List Qualtrics surveys"""
    click.secho()
    for survey in io.list_available_schemas('qualtrics'):
        click.secho(" {} : {}".format(click.style(survey.id, fg='bright_cyan'), survey.name))
    click.secho()

@cli.command()
@click.argument('instrument', shell_complete=complete_instruments)
@click.argument('version', required=False, shell_complete=complete_versions)
@click.argument('uri', required=False)
def add(instrument: str, version: str, uri: str):
    """Add instruments"""
    click.secho(instrument)
    click.secho(version)
    click.secho(uri)

@cli.command()
@click.argument('instrument', required=False, shell_complete=complete_instruments)
@click.argument('version', required=False, shell_complete=complete_versions)
def download(instrument: str, version: str):
    """Download instruments"""
    click.secho(instrument)
    click.secho(version)

@cli.command()
def sangen():
    """Generate / update sanitizers"""
    click.secho("Gen / update sanitizers")

@cli.command()
def sanitize():
    """Run sanitizers"""
    click.secho("Run sanitizers")