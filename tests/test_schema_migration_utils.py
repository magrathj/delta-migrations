import shutil 
from os import path
from data_migrations.schema_migration_table import  schema
from data_migrations.schema_migration_utils import create_migration_table

def test_creating_migration_table(spark):
    path = '\tmp\migrations/history_table'
    create_migration_table(spark, path, schema)
    assert path.exists('/tmp/migrations/history_table') == True
    shutil.rmtree('/tmp/migrations/history_table')
