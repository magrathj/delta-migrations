
import click
from . import app
from .runner import DeltaMigrationRunner
from .helper import DeltaMigrationHelper

@click.group()
def cli():
    pass


cli.add_command(app.create_migration_dir)

if __name__ == "__main__":
    cli()