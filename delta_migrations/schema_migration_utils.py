import re
import glob
import datetime
from pyspark.sql.functions import col
from concurrent.futures import ThreadPoolExecutor, wait

def get_datetime():
    """return current time"""
    return datetime.datetime.now()

def migrations_table_exists(spark, path):
    """verify if migration table exists"""
    try:
        df = spark.read.format("delta").load(path)
        return True
    except Exception as e:
        return False
  
def create_migration_table(spark, path, schema):
    """create migration table in designated path"""
    print ("Creating _migrations table...")
    (
      spark
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


def parallel_run_function(function_definition, function_info): 
    """Function which takes a function and a list of dictionaries as arguments, so to pass to multiple instances of the function"""
    pool = ThreadPoolExecutor((len(function_info)))
    futures = []
    for info in function_info:
        futures.append(pool.submit(function_definition, 
                                   function_info
                                   )
                    )
    wait(futures)
    for future in futures:
        print(future.result())


def set_delta_table_properties(spark, table_name, table_property, table_property_value):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({table_property}='{table_property_value}')")

def add_columns_to_delta_table(spark, table_name):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {data_type})")

def add_non_nullability_to_delta_table_column(spark, table_name, col_name):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} CHANGE COLUMN {col_name} ADD NOT NULL")

def drop_non_nullability_to_delta_table_column(spark, table_name, col_name):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} CHANGE COLUMN {col_name} DROP NOT NULL")

def add_check_constraint_to_delta_table_column(spark, table_name, col_name, constraint):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT {col_name} CHECK ({constraint})")

def drop_check_constraint_to_delta_table_column(spark, table_name, col_name):
    """Change Delta table properties using table name"""
    spark.sql(f"ALTER TABLE {table_name} DROP CONSTRAINT {col_name}")
    
    
def change_delta_table_column_type(spark, table_name, col_name, data_type):
    """Change column type requires overwrite of schema"""
    (
        spark.read.table(table_name)
        .withColumn(col_name, col(col_name).cast(data_type))
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

def create_delta_table(spark, path, table_name, schema):
    """create delta table in designated path"""
    print ("Creating delta table...")
    (
      spark
        .createDataFrame([], schema)
        .write
        .format("delta")
        .mode("overwrite")
        .option("path", path)
        .saveAsTable(table_name)
    )