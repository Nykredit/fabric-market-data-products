from pyspark.sql import SparkSession
from DMS_SJ_Library import mount_lakehouses

if __name__ == "__main__":

    spark = (SparkSession.builder.appName("DMS_SJ_Test").getOrCreate())
    spark_context = spark.sparkContext

    mount_lakehouses()