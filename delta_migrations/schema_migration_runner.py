from subprocess import call
from . import schema_migration_table
from . import schema_migration_utils


class MigrationScriptNotFound(Exception):
    """Migration Exception: Exception raised when migration script cannot be found."""

def run_migrations(spark, migrations_to_run, path, schema):
    """run and record migration"""
    for migration in migrations_to_run:
        result = call(["python", migration])
        if result != 0:
            raise MigrationScriptNotFound(f"Migration Exception: {migration} was not found") 
        else:
            schema_migration_utils.record_migration(spark, migration, path, schema)


def main(spark, path):
    # check if table exists
    table_exists = schema_migration_utils.migrations_table_exists(spark, path)
    
    # create table if it doesnt exist
    if not table_exists:
        schema_migration_utils.create_migration_table(spark, path, schema_migration_table.schema)
    
    # check records in table against list of migrations
    migration_list = schema_migration_utils.get_list_of_migration_scripts()
    history_df = schema_migration_utils.get_migration_records(spark, path)
    history_list = schema_migration_utils.migration_records_to_list(history_df)
    
    # retrieve list of new migrations to run
    migrations_to_run_list = schema_migration_utils.migrations_to_run(history_list, migration_list)

    # run migrations in order
    run_migrations(spark, migrations_to_run_list, path, schema_migration_table.schema)