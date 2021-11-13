import shutil 
import datetime
from os import path
from delta_migrations.delta_migration_table import  schema
from delta_migrations.delta_migration_helper import DeltaMigrationHelper
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def test_create_table(spark):
    input = {
            'table_name': 'example1',
            'path': '/tmp/bronze/delta/example1', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])

def test_new_column_to_existing_table(spark):
    # create existing table
    existing_table_input = {
            'table_name': 'example2',
            'path': '/tmp/bronze/delta/example2', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(existing_table_input)
    # add new column to delta table
    input = {
            'table_name': 'example2',
            'path': '/tmp/bronze/delta/example2', 
            'modify_type': 'add_column',
            'col_name': 'script_number',
            'data_type': 'int'
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])

def test_drop_column_in_existing_table(spark):
    # create existing table
    existing_table_input = {
            'table_name': 'example3',
            'path': '/tmp/bronze/delta/example3', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(existing_table_input)
    # add new column to delta table
    input = {
            'table_name': 'example3',
            'path': '/tmp/bronze/delta/example3', 
            'modify_type': 'drop_column',
            'col_name': 'script_name'
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])

def test_change_column_name(spark):
    # create existing table
    existing_table_input = {
            'table_name': 'example4',
            'path': '/tmp/bronze/delta/example4', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(existing_table_input)
    # add new column to delta table
    input = {
            'table_name': 'example4',
            'path': '/tmp/bronze/delta/example4', 
            'modify_type': 'change_column_name',
            'col_name': 'script_name',
            'new_col_name': 'script_name_extended'
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])

def test_change_column_type(spark):
    # create existing table
    existing_table_input = {
            'table_name': 'example5',
            'path': '/tmp/bronze/delta/example5', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(existing_table_input)
    # add new column to delta table
    input = {
            'table_name': 'example5',
            'path': '/tmp/bronze/delta/example5', 
            'modify_type': 'change_column_type',
            'col_name': 'script_name',
            'data_type': 'int'
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])


