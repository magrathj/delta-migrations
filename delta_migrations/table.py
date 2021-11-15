from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("script_name", StringType(), False),
    StructField("applied", TimestampType(), False)
  ])