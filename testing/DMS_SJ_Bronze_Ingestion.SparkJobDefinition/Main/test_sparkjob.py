import json
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col

from DMS_SJ_Bronze_Ingestion import DMSBronzeIngestionJob


@pytest.fixture
def spark_job(spark):
    """
    Fixture to create an instance of DMSBronzeIngestionJob with the Spark session 
    and mocked logger, LakehouseUtils and notebookutils.
    """
    with patch('DMS_SJ_Bronze_Ingestion.LakehouseUtils') as mock_lakehouse_utils, \
        patch('DMS_SJ_Bronze_Ingestion.notebookutils.fs.head') as mock_fs_head:

        # Mocked function calls
        mock_lakehouse_utils.get_bronze_lakehouse_path.return_value = "/mocked/path/to/bronze"
        mock_fs_head.return_value = "mocked_shared_key"

        # Create job instance and use spark fixture
        job = DMSBronzeIngestionJob()
        job._spark = spark
        job._logger = MagicMock()
        yield job


def test_setup_event_stream(spark_job):
    """
    Unittest to test the method setup_event_stream of DMSBronzeIngestionJob.
    """
    with patch.object(spark_job.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils, 'encrypt', return_value="mocked_encrypted_connection_string"), \
         patch.object(spark_job.spark, 'readStream') as mock_readStream:
        
        # Mock the DataFrame returned by readStream
        mock_df = MagicMock(spec=DataFrame)
        mock_readStream.format.return_value = mock_readStream
        mock_readStream.options.return_value = mock_readStream
        mock_readStream.load.return_value = mock_df
        
        # Mock the select method to return the mock DataFrame
        mock_df.select.return_value = mock_df
        
        # Call the method
        df_stream = spark_job.setup_event_stream()
        
        # Verify the mocked method calls
        spark_job.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt.assert_called_once_with("mocked_event_hub_connection_string")
        mock_readStream.format.assert_called_once_with("eventhubs")
        mock_readStream.options.assert_called_once_with(eventhubs_connectionString="mocked_encrypted_connection_string")
        mock_readStream.load.assert_called_once()
        mock_df.select.assert_called_once_with(
            col("body").cast("string").alias("decoded_body"),
            col("partition"),
            col("sequenceNumber")
        )
        
        # Verify the returned DataFrame
        assert df_stream == mock_df


def test_write_row_to_file(spark_job):
    """
    Unittest to test the static method write_row_to_file of DMSBronzeIngestionJob.
    """
    with patch('DMS_SJ_Bronze_Ingestion.notebookutils.fs.put') as mock_put:
        
        # Prepare testdata
        row = Row(
            decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data"}),
            BronzeCreatedAt=datetime(2025, 3, 11, 14, 52, 53),
            partition=0,
            sequenceNumber=1,
            OutputPath="/path/to/output"
        )

        # Call the function
        file_path = spark_job.write_row_to_file(row)

        # Assert the results are as expected
        expected_path = "/path/to/output/instrument/2025/03/11/event_0_1.json"
        assert file_path == expected_path

        # Verify the content written to the file
        expected_content = json.dumps({
            "Type": "Instrument",
            "data": "sample_data",
            "BronzeCreatedAt": "2025-03-11T14:52:53"
        }, indent=2)
        mock_put.assert_called_once_with(expected_path, expected_content, overwrite=True)
