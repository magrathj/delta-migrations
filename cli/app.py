import os
import click

@click.command()
@click.option(
    "--location",
    help="This specifies the location you want to create the directory",
)
def create_migration_dir(location):
    if location:    
        os.makedirs(f"{location}/migrations", exist_ok=True)
    else:
        os.makedirs(f"/migrations", exist_ok=True)


