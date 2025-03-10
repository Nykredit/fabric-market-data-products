from datetime import datetime as dt
import pytz
import json

from pyspark.sql.functions import col
from notebookutils import mssparkutils

from shared_libs import SparkJob


class BronzeIngestionJob(SparkJob):

    def __init__(self):
        self.output_base_path = "Files/dms/"

    def setup_event_stream(self):
        pass

    def write_to_file(self, df, epoch_id):
        try:
            if df.isEmpty():
                self.logger.info(f"No data received in batch {epoch_id}. Skipping save.")
                return

            self.logger.debug(f"Processing batch {epoch_id}")

            # Convert DataFrame to Pandas for row-wise iteration
            pdf = df.select("decoded_body", "sequenceNumber", "partition").toPandas()

            for _, row in pdf.iterrows():
                
                now = dt.now(tz=pytz.utc)
                json_content = json.loads(row["decoded_body"])
                json_content["BronzeCreatedAt"] = now.isoformat()

                event_type = json_content.get("Type", "unknown").lower()

                partition_path = f"{self.output_base_path}{event_type}/{now.year}/{now.month:02d}/{now.day:02d}/"
                output_path = f"{partition_path}event_{row['partition']}_{row['sequenceNumber']}.json"

                json_data = json.dumps(json_content, indent=2)

                mssparkutils.fs.put(output_path, json_data, overwrite=True)
                self.logger.info(f"Saved event to file {output_path}")

        except Exception as e:
            self.logger.error(f"Error processing batch {epoch_id}: {e}")

    def main(self):

        sharedKey = mssparkutils.fs.head(f"Files/_meta/EventHubConnection.txt")
        endpoint = "sb://nywt-dms-tst-function-eventhub.servicebus.windows.net/"
        event_hub_name = "nywt-dms-tst-function-dms"

        event_hub_connection_string = f"Endpoint={endpoint};SharedAccessKeyName=ListenSharedAccessKey;SharedAccessKey={sharedKey};EntityPath={event_hub_name}"
        eh_conf = {
            "eventhubs.connectionString": self.spark.sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string),    
        }

        # Read from Event Hub
        df_stream = (self.spark.readStream
            .format("eventhubs")
            .options(**eh_conf)    
            .load()
        )

        df_stream = df_stream.select(
            col("body").cast("string").alias("decoded_body"),    
            col("partition"),
            col("sequenceNumber"),    
        )

        started_stream = (
            df_stream.writeStream
            .foreachBatch(self.write_to_file)
            .trigger(processingTime="5 minutes")
            .start()
        )
        started_stream.awaitTermination()


if __name__ == "__main__":
    BronzeIngestionJob().main()
