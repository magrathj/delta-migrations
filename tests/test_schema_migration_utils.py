import shutil 
from os import path
from delta_migrations.schema_migration_table import  schema
from delta_migrations.schema_migration_utils import create_migration_table

def test_creating_migration_table(spark):
    migration_path = '/tmp/migrations/history_table'
    create_migration_table(spark, path, schema)
    assert path.exists(migration_path) == True
    shutil.rmtree(migration_path)
