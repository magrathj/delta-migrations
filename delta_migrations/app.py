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
    
    click.echo(f"Add delta migration starter scripts to {location}")

    zip_url = "https://github.com/magrathj/delta-migrations-template/archive/refs/heads/main.zip"
    with urlopen(zip_url) as zip_resp:
        with ZipFile(BytesIO(zip_resp.read())) as zip_file:
            zip_file.extractall(location)
            
    click.echo(f"Completed delta migration setup")
