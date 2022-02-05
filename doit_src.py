import typing as t
import click
import yaml
from pathlib import Path
from pyrsistent import PRecord, pvector, field, pvector_field

class InstrumentVersion(PRecord):
    name = field(type=str)
    uri = field(type=str)

class Instrument(PRecord):
    name = field(type=str)
    long_name = field(type=str)
    description = field(type=str)
    versions = pvector_field(InstrumentVersion)

class Context(PRecord):
    instruments = pvector_field(Instrument)


class DoitSrcEnv(PRecord):
    qualtrics_api_token = field(type=str)

dummy_context = Context(
    instruments = pvector([
        Instrument(
            name = "student_behavior",
            long_name = "Student Behavior Questionnaire",
            description = "Description of Student Behavior Questionnaire",
            versions = pvector([
                InstrumentVersion(
                    name = "Y1W1",
                    uri = "qualtrics://SV_asdfsdf"
                ),
                InstrumentVersion(
                    name = "Y1W2",
                    uri = "qualtrics://SV_asdfsdf"
                )
            ])
        ),
        Instrument(
            name = "teacher_wellbeing",
            long_name = "Teacher Wellbeing Questionnaire",
            description = "Description of Teacher Wellbeing Questionnaire"
        )
    ])
)

class RequireArgIf(click.Argument):
    def __init__(self, *args: t.Any, **kwargs: t.Any):
        self.required_if = kwargs.pop("required_if")
        assert self.required_if, "'required_if' parameter required"
        super(RequireArgIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx: click.Context, opts: t.Mapping[str, t.Any], args: t.List[str]):
        for item in self.required_if:
            if opts[item] is None:
                raise click.ClickException("{} required".format(item))
        return super(RequireArgIf, self).handle_parse_result(ctx, opts, args)

def complete_instruments(ctx: click.Context, param: click.Argument, incomplete: str):
    return [i.name for i in dummy_context.instruments if i.name.startswith(incomplete)]

def complete_versions(ctx: click.Context, param: click.Argument, incomplete: str):
    return [iv.name for i in dummy_context.instruments for iv in i.versions if i.name == ctx.params['instrument'] and iv.name.startswith(incomplete)]

CONTEXT_SETTINGS: t.Mapping[str, t.Any] = dict()

ENV_FILENAME = Path(".env.yaml")

if ENV_FILENAME.is_file():
    with open(ENV_FILENAME, "r") as stream:
        try:
            CONTEXT_SETTINGS['default_map'] = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

@click.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def cli(ctx: click.Context):
    """root doit-src description -- talk about what this thing does"""
    pass

@cli.command()
@click.argument('instrument', shell_complete=complete_instruments)
@click.argument('version', required=False, shell_complete=complete_versions)
@click.argument('uri', required=False, cls=RequireArgIf, required_if=['uri'])
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