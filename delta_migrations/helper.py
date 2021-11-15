from abc import ABC, abstractmethod
from pyspark.sql.functions import col
from concurrent.futures import ThreadPoolExecutor, wait
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class MigrationModifyTypeDoesNotExist(Exception):
    """Migration Exception: Exception raised when modify type does not exist."""

class DeltaMigrationBase(ABC):

    @abstractmethod
    def transform(cls, modify_dict: dict):
        """ Apply modifying method to delta table """
        pass

class DeltaMigrationAddColumn(DeltaMigrationBase):
    """Create """

    def __init__(self, spark):
        self.spark = spark

    def add_columns_to_delta_table(self, spark, table_name, col_name, data_type):
        """Change Delta table properties using table name"""
        print ("Creating delta table...")
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {data_type})")

    def transform(self, modify_dict: dict):
        self.add_columns_to_delta_table(self.spark, modify_dict['table_name'], modify_dict['col_name'], modify_dict['data_type'])

class DeltaMigrationChangeColumnType(DeltaMigrationBase):
    """Create """

    def __init__(self, spark):
        self.spark = spark

    def change_delta_table_column_type(self, spark, table_name, col_name, data_type):
        """Change column type requires overwrite of schema"""
        print ("Creating delta table...")
        (
            spark.read.table(table_name)
            .withColumn(col_name, col(col_name).cast(data_type))
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def transform(self, modify_dict: dict):
        self.change_delta_table_column_type(self.spark, modify_dict['table_name'], modify_dict['col_name'], modify_dict['data_type'])

class DeltaMigrationDropColumn(DeltaMigrationBase):
    """Drop column in delta table"""

    def __init__(self, spark):
        self.spark = spark

    def drop_column_delta_table(self, spark, table_name, col_name):
        """Change column type requires overwrite of schema"""
        print ("Creating delta table...")
        (
            spark.read.table(table_name)
            .drop(col_name)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def transform(self, modify_dict: dict):
        self.drop_column_delta_table(self.spark, modify_dict['table_name'], modify_dict['col_name'])

class DeltaMigrationChangeColumnName(DeltaMigrationBase):
    """Create """

    def __init__(self, spark):
        self.spark = spark

    def change_delta_table_column_name(self, spark, table_name, col_name, new_col_name):
        """Change column type requires overwrite of schema"""
        print ("Creating delta table...")
        (
            spark.read.table(table_name)
            .withColumnRenamed(col_name, new_col_name)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    def transform(self, modify_dict: dict):
        self.change_delta_table_column_name(self.spark, modify_dict['table_name'], modify_dict['col_name'], modify_dict['new_col_name'])

class DeltaMigrationCreateTable(DeltaMigrationBase):
    """Create """

    def __init__(self, spark):
        self.spark = spark

    def create_delta_table(self, spark, path, table_name, schema):
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

    def transform(self, modify_dict: dict):
        self.create_delta_table(self.spark, modify_dict['path'], modify_dict['table_name'], modify_dict['schema'])


class DeltaMigrationHelper:
    """Delta migration helper class to help creating migrations"""

    MODIFY_TYPE = ['create_table', 'add_column', 'drop_column', 'change_column_type', 'change_column_name', 'add_constraint']
    
    def __init__(self, spark):
        self.spark = spark

    def get_modify_delta_table_dictionary(self):
        modify_delta_table_dict = {
            'create_table':{
                'required_columns': [
                    'table_name',
                    'schema'
                ],
                'optional_columns': [
                    'partition_by'
                ],
                'function_mapping': DeltaMigrationCreateTable
            },
            'add_column':{
                'required_columns': [
                    'table_name',
                    'col_name',
                    'data_type'
                ],
                'optional_columns': [

                ],
                'function_mapping': DeltaMigrationAddColumn
            },
            'drop_column':{
                'required_columns': [
                    'table_name',
                    'col_name'
                ],
                'optional_columns': [
                    
                ],
                'function_mapping': DeltaMigrationDropColumn
            },
            'change_column_type':{
                'required_columns': [
                    'table_name',
                    'col_name',
                    'data_type'
                ],
                'optional_columns': [
                    
                ],
                'function_mapping': DeltaMigrationChangeColumnType
            },
            'change_column_name':{
                'required_columns': [
                    'table_name',
                    'col_name',
                    'new_col_name'
                ],
                'optional_columns': [
                    
                ],
                'function_mapping': DeltaMigrationChangeColumnName
            }
        }
        return modify_delta_table_dict

    def modify_delta_table(self, modify_dict: dict):
        """

        table_name - required str of the name of the delta table
        path - required str of the location of the delta table
        modify_type - required str of what modification you want to apply to the delta table, can be one of the following ['create_table', 'add_column', 'drop_column', 'change_column_type', 'change_column_name', 'add_constraint', 'drop_constraint', 'add_table_property', 'drop_column_non_nullability' ]
        schema - optional pyspark.type object describing the model of the table. Required for modify_type='create_table'
        partition_by - optional str of column_names to partition the delta table by
        col_name - optional str of column_name to modify
        new_col_name - optional str of new column to rename too
        data_type - optional str of column type to change too


        ['create_table', 'add_column', 'drop_column', 'change_column_type', 'change_column_name', 'add_constraint']
        Example:
        
        # create a new table
        {
            'table_name': 'example1',
            'path': '/mnt/bronze/delta/example1', 
            'modify_type': 'create_table'
            'schema': StructType([StructField("script_name", StringType(), False), StructField("applied", TimestampType(), False)])
            'partition_by': ["script_name"],
        },
        # add a new column to an existing table
        {
            'table_name': 'example2',
            'path': '/mnt/bronze/delta/example2', 
            'modify_type': 'add_column',
            'col_name': 'script_number,
            'data_type': 'int'
        },
        # drop column of an existing table
        {
            'table_name': 'example3',
            'path': '/mnt/bronze/delta/example3', 
            'modify_type': 'drop_column',
            'col_name': 'script_name'
        },
        # change column name of an existing table
        {
            'table_name': 'example4',
            'path': '/mnt/bronze/delta/example4', 
            'modify_type': 'change_column_name',
            'col_name': 'script_name',
            'new_col_name': 'script_name_extended'
        },
        # change column type of an existing table
        {
            'table_name': 'example5',
            'path': '/mnt/bronze/delta/example5', 
            'modify_type': 'change_column_type',
            'col_name': 'script_name',
            'data_type': 'int'
        }
    
        """
        modify_delta_table_dict = self.get_modify_delta_table_dictionary()
        modify_type = modify_dict['modify_type']
        # check if modify_type is available in config
        if not modify_type:
            raise MigrationModifyTypeDoesNotExist(f"Modify type is missing or doesnt exist. Please use one of the following {self.MODIFY_TYPE}")
        # get mapping class
        modify_func_class = modify_delta_table_dict[modify_type]['function_mapping']
        # apply transform from mapping class
        modify_func_class(self.spark).transform(modify_dict)


