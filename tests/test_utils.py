import shutil 
import datetime
from os import path
from delta_migrations.table import  schema
from delta_migrations.utils import create_migration_table, record_migration, get_migration_records, migration_records_to_list, migrations_to_run, migrations_table_exists

def test_creating_migration_table(spark):
    migration_path = '/tmp/migrations/history_table'
    create_migration_table(spark, migration_path, schema)
    assert path.exists(migration_path) == True
    shutil.rmtree(migration_path)


def test_record_migration(spark):
    script_name = '0001_migration.py'
    migration_path = '/tmp/migrations/history_table'
    create_migration_table(spark, migration_path, schema)
    record_migration(spark, script_name, migration_path, schema)
    df = spark.read.format("delta").load(migration_path)
    results = df.collect()
    assert results[0].script_name == script_name
    assert results[0].applied is not None
    shutil.rmtree(migration_path)


def test_get_migration_records(spark):
    script_name_1 = '0001_migration.py'
    script_name_2 = '0002_migration.py'
    migration_path = '/tmp/migrations/history_table'
    create_migration_table(spark, migration_path, schema)
    record_migration(spark, script_name_1, migration_path, schema)
    record_migration(spark, script_name_2, migration_path, schema)
    df = get_migration_records(spark, migration_path)
    results = df.collect()
    assert results[0].script_name == script_name_1
    assert results[0].applied is not None
    assert results[1].script_name == script_name_2
    assert results[1].applied is not None
    shutil.rmtree(migration_path)


def test_migration_records_to_list(spark):
    script_name = '0001_migration.py'
    time_now = datetime.datetime.now()
    migration_records_df = spark.createDataFrame([(script_name, time_now)], schema)
    migration_list = migration_records_to_list(migration_records_df)
    assert migration_list == [(script_name)]


def test_migrations_to_run(spark):
    migration_records_list = ['0001_migration.py']
    migration_list         = ['0001_migration.py', '0002_migration.py']
    results = migrations_to_run(migration_records_list, migration_list)
    assert results == ['0002_migration.py']


def test_migrations_table_exists_true(spark):
    migration_path = '/tmp/migrations/history_table'
    create_migration_table(spark, migration_path, schema)
    assert migrations_table_exists(spark, migration_path) == True
    shutil.rmtree(migration_path)


def test_migrations_table_exists_false(spark):
    migration_path = '/tmp/migrations/history_table'
    assert migrations_table_exists(spark, migration_path) == False