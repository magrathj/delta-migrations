import shutil 
from os import path
from delta_migrations import app
from click.testing import CliRunner

def test_creating_migration_dir_without_location():
    """test creation of history table without a path"""
    runner = CliRunner()
    result = runner.invoke(app.create_migration_dir)
    assert not result.exception
    assert path.exists('/tmp/delta_migrations') == True
    shutil.rmtree('/tmp/delta_migrations')

def test_creating_migration_dir_with_location():
    """test creation of history table with a path"""
    runner = CliRunner()
    result = runner.invoke(app.create_migration_dir, ['--location', '/tmp/new_delta_migrations'])
    assert not result.exception
    assert path.exists('/tmp/new_delta_migrations') == True
    shutil.rmtree('/tmp/new_delta_migrations')