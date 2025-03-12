import json

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql import DataFrame, Row
import notebookutils

from shared_libs import SparkJob, LakehouseUtils


class DMSBronzeIngestionJob(SparkJob):
    """
    A job to ingest data from the DSM EventHub and save each event as a separate JSON file
    in the bronze layer Lakehouse.
    """

    def __init__(self):
        """
        Initializes DMSBronzeIngestionJob with EventHub connection details and paths.
        """
        bronze_lh_abfspath = LakehouseUtils.get_lakehouse_path_by_keyword("bronze")
        self.output_base_path = f"{bronze_lh_abfspath}/Files/dms"
        self.checkpoint_location = "Files/dms/_meta/bronze_ingestion_checkpoint"

        # TODO: The following should be in KeyVault or use another auth method
        sharedKey = notebookutils.fs.head(f"Files/_meta/EventHubConnection.txt")
        endpoint = "sb://nywt-dms-tst-function-eventhub.servicebus.windows.net/"
        event_hub_name = "nywt-dms-tst-function-dms"
        self.event_hub_connection_string = (
            f"Endpoint={endpoint};"
            "SharedAccessKeyName=ListenSharedAccessKey;"
            f"SharedAccessKey={sharedKey};"
            f"EntityPath={event_hub_name}"
        )

    def setup_event_stream(self) -> DataFrame:
        """
        Sets up the streaming DataFrame from EventHub.

        Returns
        -------
        pyspark.sql.DataFrame
            Streaming DataFrame with selected columns.
        """
        encrypted_connection_string = self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.event_hub_connection_string)
        eh_conf = {"eventhubs.connectionString": encrypted_connection_string}
        df_stream = (
            self.spark.readStream
            .format("eventhubs")
            .options(**eh_conf)    
            .load()
        )

        return df_stream.select(
            col("body").cast("string").alias("decoded_body"),    
            col("partition"),
            col("sequenceNumber"),    
        )

    @staticmethod
    def write_row_to_file(row: Row):
        """
        Writes a single row to a JSON file. The BronzeCreatedAt is inserted into the json document.

        The full path for the output file is constructed as:
            {OutputPath}/{Type}/{YYYY/MM/DD}/event_{partition}_{sequenceNumber}.json
        where:
        - Type is extracted from the decoded_body JSON.
        - YYYY/MM/DD is the formatted BronzeCreatedAt timestamp.
        - partition and sequenceNumber are used to create a unique filename.

        Parameters
        ----------
        row : pyspark.sql.Row
            Row containing the event data.
        """
        json_content = json.loads(row["decoded_body"])
        bronze_created_at_str = row["BronzeCreatedAt"].strftime("%Y-%m-%dT%H:%M:%S")
        json_content["BronzeCreatedAt"] = bronze_created_at_str
        json_data = json.dumps(json_content, indent=2)

        # Create the entire path from the row data and job time
        event_type = json_content.get('Type', 'unknown').lower()
        formatted_date = row["BronzeCreatedAt"].strftime("%Y/%m/%d")
        full_path = (
            f"{row['OutputPath']}/{event_type}/"
            f"{formatted_date}/event_{row['partition']}_{row['sequenceNumber']}.json"
        )

        notebookutils.fs.put(full_path, json_data, overwrite=True)
        return full_path

    def process_batch(self, df: DataFrame, epoch_id: int):
        """
        Processes each batch of data and writes rows to files.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            DataFrame containing the batch data.
        epoch_id : int
            ID of the current batch.
        """
        try:
            if df.isEmpty():
                self.logger.info(f"No data received in batch {epoch_id}. Skipping save.")
                return

            self.logger.debug(f"Processing batch {epoch_id}")
            df_with_path = (
                df.select("decoded_body", "sequenceNumber", "partition")
                .withColumn("BronzeCreatedAt", current_timestamp())
                .withColumn("OutputPath", lit(self.output_base_path))
            )

            # Iterate over rows and write each row to a file. Done on driver due to collect.
            for row in df_with_path.collect():
                file_path = self.write_row_to_file(row)
                self.logger.info(f"Saved the json file: {file_path}")

        except Exception as e:
            self.logger.error(f"Error processing batch {epoch_id}: {e}")

    def main(self):
        """
        Main method to start the streaming job.
        """
        df_stream = self.setup_event_stream()
        started_stream = (
            df_stream.writeStream
            .option("checkpointLocation", self.checkpoint_location)
            .foreachBatch(self.process_batch)
            .trigger(processingTime="10 minutes")
            .start()
        )
        started_stream.awaitTermination()


if __name__ == "__main__":
    DMSBronzeIngestionJob().main()
