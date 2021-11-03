import re
import glob
import datetime
from subprocess import call
from pyspark.sql.functions import col

def get_datetime():
    """return current time"""
    return datetime.datetime.now()

def migrations_table_exists(spark, path):
    """verify if migration table exists"""
    pass
  
def create_migration_table(spark, path, schema):
    """create migration table in designated path"""
    print ("Creating _migrations table...")
    (spark
    .createDataFrame([], schema)
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", path)
    .saveAsTable("migrations")
    )    
    

def record_migration(spark, script_name, path, schema):
    """record migration to history table"""
    (spark
    .createDataFrame([[script_name, get_datetime()]], schema)
    .write
    .mode('append')
    .format("delta")
    .save(path))

def get_migration_records(spark, path):
    """get all migrations from history table"""
    migration_records_df = (spark
                            .read
                            .format("delta")
                            .load(path)
                            .orderBy(col("script_name")))
    return migration_records_df

def migration_records_to_list(migration_records_df): 
    """convert dataframe to list"""   
    migration_records_list   = [(row.script_name) for row in migration_records_df.select("script_name").collect()]
    return migration_records_list

def get_list_of_migration_scripts():
    """Run in the directory where the _migration.py scripts are available"""
    migration_scripts = glob.glob('*_migration.py')
    return migration_scripts

def migrations_to_run(migration_records_list, migration_list):
    """find which migrations are not in the history table, so as to find only new migrations"""
    migrations_to_run        = list(set(migration_list) - set(migration_records_list))
    migrations_to_run_sorted = sorted(migrations_to_run, key=lambda x:int(re.match(r'(\d+)',x).groups()[0])) 
    return migrations_to_run_sorted

def run_migrations(migrations_to_run):
    """run and record migration"""
    for migration in migrations_to_run:
        try:
            call(["python", migration])
        except Exception as e:
            raise Exception(f"Migration: {migration} failed with the exception {e}") 
        else:
            record_migration(migration)

