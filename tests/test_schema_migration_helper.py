import shutil 
import datetime
from os import path
from delta_migrations.schema_migration_table import  schema
from delta_migrations.schema_migration_helper import DeltaMigrationHelper
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def test_create_table(spark):
    input = {
            'table_name': 'example1',
            'path': '/tmp/bronze/delta/example1', 
            'modify_type': 'create_table',
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)]),
            'partition_by': ["script_name"],
        }
    x = DeltaMigrationHelper(spark)
    x.modify_delta_table(input)
    assert path.exists(input['path']) == True
    shutil.rmtree(input['path'])