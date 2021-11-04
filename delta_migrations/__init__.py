
import click
from . import app
from .schema_migration_runner import main

@click.group()
def cli():
    pass


cli.add_command(app.create_migration_dir)

if __name__ == "__main__":
    cli()