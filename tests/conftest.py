
import pytest
import pyspark
from delta import *

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