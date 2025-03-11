import json
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import Row

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

def test_process_batch_empty_df(spark_job):
    """
    Unittest to test the method process_batch of DMSBronzeIngestionJob with an empty DataFrame.
    """

    # Create an empty DataFrame
    empty_df = spark_job.spark.createDataFrame(
        [], schema="decoded_body STRING, sequenceNumber INT, partition INT"
    )

    # Call the method with an empty DataFrame
    spark_job.process_batch(empty_df, epoch_id=1)

    # Verify the logger calls
    spark_job.logger.info.assert_called_once_with("No data received in batch 1. Skipping save.")
    spark_job.logger.debug.assert_not_called()
    spark_job.logger.error.assert_not_called()

def test_process_batch_non_empty_df(spark_job):
    """
    Unittest to test the method process_batch of DMSBronzeIngestionJob with a non-empty DataFrame.
    """
    
    # Prepare testdata
    data = [
        Row(
            decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data_1"}),
            sequenceNumber=1,
            partition=0,
            other_col=""
        ),
        Row(
            decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data_2"}),
            sequenceNumber=2,
            partition=1,
            other_col=""
        )
    ]
    non_empty_df = spark_job.spark.createDataFrame(data)

    # Mock the write_row_to_file method
    spark_job.write_row_to_file = MagicMock()
    spark_job.write_row_to_file.side_effect = [
        "/path/to/output/instrument/2025/03/11/event_0_1.json",
        "/path/to/output/instrument/2025/03/11/event_1_2.json"
    ]

    # Call the method with a non-empty DataFrame
    spark_job.process_batch(non_empty_df, epoch_id=1)

    # Verify the logger calls
    spark_job.logger.debug.assert_called_once_with("Processing batch 1")
    spark_job.logger.info.assert_any_call("Saved the json file: /path/to/output/instrument/2025/03/11/event_0_1.json")
    spark_job.logger.info.assert_any_call("Saved the json file: /path/to/output/instrument/2025/03/11/event_1_2.json")
    spark_job.logger.error.assert_not_called()

    # Verify the write_row_to_file method call
    assert spark_job.write_row_to_file.call_count == 2
    
    # Create expected Rows
    expected_data = [
        Row(
            decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data_1"}),
            sequenceNumber=1,
            partition=0,
            BronzeCreatedAt=None,
            OutputPath="/mocked/path/to/bronze/Files/dms"
        ),
        Row(
            decoded_body=json.dumps({"Type": "Instrument", "data": "sample_data_2"}),
            sequenceNumber=2,
            partition=1,
            BronzeCreatedAt=None,
            OutputPath="/mocked/path/to/bronze/Files/dms"
        )
    ]

    for call, expected_row in zip(spark_job.write_row_to_file.call_args_list, expected_data):
        called_row = call[0][0].asDict()
        expected_row_dict = expected_row.asDict()
        for field in expected_row_dict:
            if field != "BronzeCreatedAt":
                assert expected_row_dict[field] == called_row[field]
