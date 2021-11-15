import shutil 
import datetime
from os import path
from delta_migrations.table import  schema
from delta_migrations.helper import DeltaMigrationHelper
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def create_existing_table(spark, table_name):
    """Helper function to setup existing test table"""
    # create existing table
    existing_table_input = {
            'table_name': table_name,
            'path': f'/tmp/bronze/delta/{table_name}', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    delta_migration_helper = DeltaMigrationHelper(spark)
    delta_migration_helper.modify_delta_table(existing_table_input)

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
    create_existing_table(spark, 'example2')
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
    assert len(spark.read.format("delta").load(input['path']).columns) == 3
    shutil.rmtree(input['path'])

def test_drop_column_in_existing_table(spark):
    # create existing table
    create_existing_table(spark, 'example3')
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
    assert len(spark.read.format("delta").load(input['path']).columns) == 1
    shutil.rmtree(input['path'])

def test_change_column_name(spark):
    # create existing table
    create_existing_table(spark, 'example4')
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
    assert len(spark.read.format("delta").load(input['path']).columns) == 2
    assert spark.read.format("delta").load(input['path']).columns == ['script_name_extended', 'applied']
    shutil.rmtree(input['path'])

def test_change_column_type(spark):
    # create existing table
    create_existing_table(spark, 'example5')
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
    assert dict(spark.read.format("delta").load(input['path']).dtypes)['script_name'] == "int" 
    shutil.rmtree(input['path'])


