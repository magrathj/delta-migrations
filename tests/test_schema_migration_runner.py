import os
import glob
import pytest
import shutil
import pyspark
from delta import *
from pyspark.sql.functions import col
from delta_migrations.schema_migration_runner import main
from delta_migrations.schema_migration_table import  schema

class Helper:

    def __init__(self, spark, path, schema):
        self.spark   = spark
        self.path    = path
        self.schema  = schema 

    def create_migration_script(self, script_name="0001_migration.py"):
        f = open(script_name, "a")
        f.close()

    def tear_down_delta_table(self):
        try:
            shutil.rmtree(self.path)
        except:
            pass

    def tear_down_migration_scripts(self):
        migration_scripts = glob.glob('*_migration.py')
        if not migration_scripts:
            return None
        for script in migration_scripts:
            os.remove(script) 
        return None

    def tear_down(self):
        self.tear_down_delta_table()
        self.tear_down_migration_scripts()

    def create_history_table(self):
        (
         self.spark
             .createDataFrame([], self.schema)
             .write
             .format("delta")
             .mode("overwrite")
             .option("path", self.path)
             .saveAsTable("migrations")
        ) 

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    tmp_path = str(tmp_path_factory.mktemp("spark_databases"))

    builder = (
        pyspark.sql.SparkSession.builder
        .master("local[1]")
        .appName("delta_migrations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark

@pytest.fixture()
def resource(request):
    print("setup")

    def teardown():
        print("teardown")
        migration_path = '/tmp/migrations/history_table'
        migration_helper = Helper(spark, migration_path, schema)
        migration_helper.tear_down()
    request.addfinalizer(teardown)
    
    return "resource"


class TestMigrations:
        
    def test_main_new_migration_added(self, resource, spark):
        migration_path = '/tmp/migrations/history_table'
        script_name_1    = "0001_migration.py"
        script_name_2    = "0002_migration.py"
        migration_helper = Helper(spark, migration_path, schema)
        migration_helper.create_history_table()
        migration_helper.create_migration_script(script_name_1)
        migration_helper.create_migration_script(script_name_2)
        main(spark, migration_path)
        df = spark.read.format("delta").load(migration_path).orderBy(col("script_name"))
        results = df.collect()
        assert results[1].script_name == script_name_2
        migration_helper.tear_down()

    def test_main_no_history_table(self, resource, spark):
        migration_path = '/tmp/migrations/history_table'
        script_name    = "0001_migration.py"
        migration_helper = Helper(spark, migration_path, schema)
        migration_helper.create_migration_script(script_name)
        main(spark, migration_path)
        df = spark.read.format("delta").load(migration_path)
        results = df.collect()
        assert results[0].script_name == script_name
        migration_helper.tear_down()