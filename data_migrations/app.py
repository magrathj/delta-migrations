import os
import click

@click.command()
@click.option(
    "--location",
    default="/tmp/delta_migrations",
    help="This specifies the location you want to create the directory",
)
def create_migration_dir(location):
    click.echo(f"Creating directory at {location}")
    os.makedirs(f"{location}/migrations", exist_ok=True)