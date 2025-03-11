import shutil
import os
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import DataFrame, Row
from delta.tables import DeltaTable

from DMS_SJ_Bronze_Ingestion import DMSBronzeIngestionJob


@pytest.fixture
def spark_job(spark):
    """
    Fixture to create an instance of DMSBronzeIngestionJob with the Spark session 
    and a mocked logger.
    """
    job = DMSBronzeIngestionJob()
    job.spark = spark
    job.logger = MagicMock()
    yield job


def test_write_row_to_file():
    """"""

    # Prepare testdata
    row = Row(
        decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data"}),
        BronzeCreatedAt=datetime(2025, 3, 11, 14, 52, 53),
        partition=0,
        sequenceNumber=1,
        OutputPath="/path/to/output"
    )

    with patch('notebookutils.fs.put') as mock_put:
        # Run function
        file_path = spark_job.write_row_to_file(row)

        # Assert the results are as expected
        expected_path = "/path/to/output/instrument/2025/03/11/event_0_1.json"
        assert file_path == expected_path

        # Verify the content written to the file
        expected_content = json.dumps({
            "Type": "test_event",
            "data": "sample_data",
            "BronzeCreatedAt": "2025-03-11T14:52:53"
        }, indent=2)
        mock_put.assert_called_once_with(expected_path, expected_content, overwrite=True)

# @pytest.fixture(autouse=True)
# def cleanup(test_job):
#     """
#     Fixture to clean up the local Delta table directories before and after each test.

#     Parameters
#     ----------
#     test_job : TestJob
#         The TestJob fixture.
#     """
#     # Cleanup before each test
#     if os.path.exists(test_job.lakehouse_path):
#         shutil.rmtree(test_job.lakehouse_path)

#     # Runs the test here
#     yield

#     # Cleanup after each test
#     if os.path.exists(test_job.lakehouse_path):
#         shutil.rmtree(test_job.lakehouse_path)

# def test_create_test_data(test_job) -> bool:
#     """
#     Test the create_test_data method of TestJob.

#     Parameters
#     ----------
#     test_job : TestJob
#         The TestJob fixture.

#     Asserts
#     -------
#     bool
#         True if the Delta table is created and the row count is correct.
#     """
#     test_job.create_test_data()
    
#     # Check if the table was created
#     assert DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_path)
    
#     # Read the data and check the row count
#     df = test_job.spark.read.format("delta").load(test_job.test_table_path)
#     assert df.count() == 10

# def test_transformation(test_job):
#     """
#     Test the transformation method of TestJob.

#     Parameters
#     ----------
#     test_job : TestJob
#         The TestJob fixture.

#     Asserts
#     -------
#     bool
#         True if the new Delta table is created, the row count is correct, and the schema is as expected.
#     """
#     test_job.create_test_data()
#     test_job.transformation()
    
#     # Check if the new table was created
#     assert DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_new_path)

#     # Read the transformed data and check the row count is 30 as expected
#     df = test_job.spark.read.format("delta").load(test_job.test_table_new_path)
#     assert df.count() == 30  

#     # Check the schema of the transformed data
#     expected_columns = ["col1", "col2", "col3_exploded", "nested_col1", "nested_col2", "col5"]
#     assert all(col in df.columns for col in expected_columns)

# def test_main(test_job):
#     """
#     Test the main method of TestJob.

#     Parameters
#     ----------
#     test_job : TestJob
#         The TestJob fixture.

#     Asserts
#     -------
#     bool
#         True if the main method runs successfully and the transformations are applied.
#     """
#     # Check that there are not tables before we start
#     assert not DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_path)
#     assert not DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_new_path)

#     # Run the method
#     test_job.main()

#     # Check if the test_table was created
#     assert DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_path)
    
#     # Read the data and check the row count
#     df = test_job.spark.read.format("delta").load(test_job.test_table_path)
#     assert df.count() == 10

#     # Check if the new table was created
#     assert DeltaTable.isDeltaTable(test_job.spark, test_job.test_table_new_path)

#     # Read the transformed data and check the row count is 30 as expected
#     df_transformed = test_job.spark.read.format("delta").load(test_job.test_table_new_path)
#     assert df_transformed.count() == 30  

#     # Check the schema of the transformed data
#     expected_columns = ["col1", "col2", "col3_exploded", "nested_col1", "nested_col2", "col5"]
#     assert all(col in df_transformed.columns for col in expected_columns)

#     # Verify log messages
#     test_job.logger.info.assert_any_call("Creating test_table")
#     test_job.logger.info.assert_any_call("Starting transformation")
#     test_job.logger.info.assert_any_call("Transformation complete and data written to test_table_new")
