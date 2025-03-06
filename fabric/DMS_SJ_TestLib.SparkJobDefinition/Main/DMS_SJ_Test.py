from pyspark.sql import SparkSession
from DMS_SJ_Library import print_hello_world

if __name__ == "__main__":

    spark = (SparkSession.builder.appName("DMS_SJ_Bronze_Ingestion").getOrCreate())
    spark_context = spark.sparkContext

    print("Hello World")
    print_hello_world()