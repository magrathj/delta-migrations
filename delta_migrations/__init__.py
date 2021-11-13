
import click
from . import app
from .delta_migration_runner import DeltaMigrationRunner
from .delta_migration_helper import DeltaMigrationHelper

@click.group()
def cli():
    pass


cli.add_command(app.create_migration_dir)

if __name__ == "__main__":
    cli()