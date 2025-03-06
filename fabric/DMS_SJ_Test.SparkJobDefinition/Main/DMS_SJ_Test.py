from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime as dt
from notebookutils import mssparkutils

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
shared_path = os.path.abspath(os.path.join(current_dir, "..", "..", "shared"))
sys.path.append(shared_path)

import test


if __name__ == "__main__":

    # Spark session builder, appName appears in log entries
    spark = (SparkSession.builder.appName("DMS_SJ_Test").getOrCreate())
    spark_context = spark.sparkContext
    
    test.hello_world()
