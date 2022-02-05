import click

from .config import load_defaults
from .utils import complete_instruments, complete_versions
from .context import load_study_context

@click.group(context_settings={ "default_map": load_defaults(), "obj": load_study_context() })
def cli():
    """root doit-src description -- talk about what this thing does"""
    pass

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
@click.option('--qualtrics-api-token')
def download(instrument: str, version: str, qualtrics_api_token: str):
    """Download instruments"""
    click.secho(instrument)
    click.secho(version)
    click.secho(qualtrics_api_token)

@cli.command()
def sangen():
    """Generate / update sanitizers"""
    click.secho("Gen / update sanitizers")

@cli.command()
def sanitize():
    """Run sanitizers"""
    click.secho("Run sanitizers")