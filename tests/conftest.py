
import pytest
import pyspark

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    tmp_path = str(tmp_path_factory.mktemp("spark_databases"))

    spark_session = (
        pyspark.sql.SparkSession.builder
        .master("local[1]")
        .appName("delta_migrations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.jars.packages", "com.databricks:spark-xml_2.11:0.5.0,io.delta:delta-core_2.12:0.7.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1")
        .config("spark.sql.warehouse.dir", tmp_path)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={tmp_path}")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield spark_session