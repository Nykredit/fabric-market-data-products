"""
This module sets up the necessary configurations and fixtures for running PySpark tests with pytest.

It includes:
- Mocking the `notebookutils` module to simulate the lakehouse environment.
- A pytest fixture to create and yield a Spark session configured for Delta Lake.
"""

from pyspark.sql import SparkSession
from unittest.mock import MagicMock
import pytest
import sys


# Mock the notebookutils module before any imports or tests are run as it only exists in Fabric.
mock_notebookutils = MagicMock()
sys.modules['notebookutils'] = mock_notebookutils


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create a Spark session for testing.

    Yields
    ------
    SparkSession
        A Spark session for testing.
    """
    spark = (
        SparkSession.builder
        .appName("TestJobTest")
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") # DeltaLake version matches Fabric
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()
