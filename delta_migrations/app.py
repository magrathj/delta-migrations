import os    
import glob
import click
from subprocess import call

@click.command()
@click.option(
    "--location",
    default="/tmp/delta_migrations",
    help="This specifies the location you want to create the directory",
)
def create_migration_dir(location):
    click.echo(f"Creating directory at {location}")
    os.makedirs(f"{location}/migrations", exist_ok=True)


@click.command()
@click.option(
    "--location",
    default="/tmp/delta_migrations",
    help="This specifies the location where you are storing the migration scripts",
)
def run_migrations():
    migration_scripts = glob.glob('*_migration_*.py')
    for script in migration_scripts:
        call(["python", script])