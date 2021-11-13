from subprocess import call
from . import delta_migration_table
from . import delta_migration_utils


class MigrationScriptNotFound(Exception):
    """Migration Exception: Exception raised when migration script cannot be found."""

class DeltaMigrationRunner:
    """Responsible for running all delta migrations"""

    def __init__(self, spark, path):
        self.spark = spark
        self.path  = path

    def run_migrations(self, spark, migrations_to_run, path, schema):
        """run and record migration"""
        for migration in migrations_to_run:
            result = call(["python", migration])
            if result != 0:
                raise MigrationScriptNotFound(f"Migration Exception: {migration} was not found") 
            else:
                delta_migration_utils.record_migration(spark, migration, path, schema)


    def main(self):
        # check if table exists
        table_exists = delta_migration_utils.migrations_table_exists(self.spark, self.path)
        
        # create table if it doesnt exist
        if not table_exists:
            delta_migration_utils.create_migration_table(self.spark, self.path, delta_migration_table.schema)
        
        # check records in table against list of migrations
        migration_list = delta_migration_utils.get_list_of_migration_scripts()
        history_df = delta_migration_utils.get_migration_records(self.spark, self.path)
        history_list = delta_migration_utils.migration_records_to_list(history_df)
        
        # retrieve list of new migrations to run
        migrations_to_run_list = delta_migration_utils.migrations_to_run(history_list, migration_list)

        # run migrations in order
        self.run_migrations(self.spark, migrations_to_run_list, self.path, delta_migration_table.schema)