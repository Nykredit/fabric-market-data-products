from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode, col
from delta.tables import DeltaTable
import notebookutils

from shared_libs import SparkJob


class TestJob(SparkJob):
    """
    Example implementation of a Spark job.

    This class demonstrates how to create and transform data using Spark and Delta Lake.
    """

    def __init__(self):
        """
        Initializes the TestJob with job name and paths.
        """
        self.job_name = "dms_test_transformation"
        self.lakehouse_path = notebookutils.lakehouse.list()[0]['properties']['abfsPath']
        self.test_table_path = f"{self.lakehouse_path}/Tables/test_table"
        self.test_table_new_path = f"{self.lakehouse_path}/Tables/test_table_new"

    def create_test_data(self):
        """
        Creates test data and writes it to a Delta table.
        """
        # Define schema
        schema = StructType([
            StructField("col1", IntegerType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", ArrayType(IntegerType()), True),
            StructField("col4", StructType([
                StructField("nested_col1", StringType(), True),
                StructField("nested_col2", IntegerType(), True)
            ]), True),
            StructField("col5", StringType(), True)
        ])

        # Create data
        data = [
            (1, "a", [1, 2, 3], {"nested_col1": "x", "nested_col2": 10}, "alpha"),
            (2, "b", [4, 5, 6], {"nested_col1": "y", "nested_col2": 20}, "beta"),
            (3, "c", [7, 8, 9], {"nested_col1": "z", "nested_col2": 30}, "gamma"),
            (4, "d", [10, 11, 12], {"nested_col1": "w", "nested_col2": 40}, "delta"),
            (5, "e", [13, 14, 15], {"nested_col1": "v", "nested_col2": 50}, "epsilon"),
            (6, "f", [16, 17, 18], {"nested_col1": "u", "nested_col2": 60}, "zeta"),
            (7, "g", [19, 20, 21], {"nested_col1": "t", "nested_col2": 70}, "eta"),
            (8, "h", [22, 23, 24], {"nested_col1": "s", "nested_col2": 80}, "theta"),
            (9, "i", [25, 26, 27], {"nested_col1": "r", "nested_col2": 90}, "iota"),
            (10, "j", [28, 29, 30], {"nested_col1": "q", "nested_col2": 100}, "kappa")
        ]

        # Create DataFrame
        df = self.spark.createDataFrame(data, schema)
        df.write.format("delta").mode("overwrite").save(self.test_table_path)
                        
    def transformation(self):
        """
        Reads, transforms, and writes data to a new Delta table.
        """
        # Read the existing table
        df = self.spark.read.format("delta").load(self.test_table_path)

        # Flatten the nested column
        df_flattened = df.select(
            "col1",
            "col2",
            "col3",
            col("col4.nested_col1").alias("nested_col1"),
            col("col4.nested_col2").alias("nested_col2"),
            "col5"
        )

        # Unpivot the array column
        df_unpivoted = df_flattened.withColumn("col3_exploded", explode("col3"))

        # Write the transformed data to a new Delta table
        df_unpivoted.write.format("delta").mode("overwrite").save(self.test_table_new_path)

    def main(self):
        """
        Main method to run the job.
        """
        self.setup_spark(self.job_name)

        if not DeltaTable.isDeltaTable(self.spark, self.test_table_path):
            self.logger.info("Creating test_table")
            self.create_test_data()
        else:
            self.logger.info("test_table already exist")

        self.logger.info("Starting transformation")
        self.transformation()
        self.logger.info("Transformation complete and data written to test_table_new")
