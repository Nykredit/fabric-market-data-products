from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime as dt
import pytz
import json
from notebookutils import mssparkutils

if __name__ == "__main__":

    # Spark session builder, appName appears in log entries
    spark = (SparkSession.builder.appName("DMS_SJ_Bronze_Ingestion").getOrCreate())
    spark_context = spark.sparkContext

    # Setup logging
    log4jLogger = spark_context._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(spark.conf.get("spark.app.name"))
    logger.setLevel(log4jLogger.Level.INFO)

    with open("/dbfs/lakehouse/default/Files/_meta/EventHubConnection.txt", "r") as file:
        sharedKey = file.readline() 
    
    endpoint = "sb://nywt-dms-tst-function-eventhub.servicebus.windows.net/"
    event_hub_name = "nywt-dms-tst-function-dms"

    event_hub_connection_string = f"Endpoint={endpoint};SharedAccessKeyName=ListenSharedAccessKey;SharedAccessKey={sharedKey};EntityPath={event_hub_name}"

    eh_conf = {
        "eventhubs.connectionString": spark_context._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string),    
    }

    output_base_path = "Files/dms/"

    def write_to_file(df, epoch_id):
        try:
            if df.isEmpty():
                logger.info(f"No data received in batch {epoch_id}. Skipping save.")
                return

            logger.debug(f"Processing batch {epoch_id}")

            # Convert DataFrame to Pandas for row-wise iteration
            pdf = df.select("decoded_body", "sequenceNumber", "partition").toPandas()

            for _, row in pdf.iterrows():
                
                now = dt.now(tz=pytz.utc)
                json_content = json.loads(row["decoded_body"])
                json_content["BronzeCreatedAt"] = now.isoformat()

                event_type = json_content.get("Type", "unknown").lower()

                partition_path = f"{output_base_path}{event_type}/{now.year}/{now.month:02d}/{now.day:02d}/"
                output_path = f"{partition_path}event_{row['partition']}_{row['sequenceNumber']}.json"

                json_data = json.dumps(json_content, indent=2)

                mssparkutils.fs.put(output_path, json_data, overwrite=True)
                logger.info(f"Saved event to file {output_path}")
                print(f"Saved event to file {output_path}")
        except Exception as e:
            logger.error(f"Error processing batch {epoch_id}: {e}")
            print(f"ERROR: Processing batch {epoch_id}: {e}")

    # Read from Event Hub
    df_stream = (spark.readStream
        .format("eventhubs")
        .options(**eh_conf)    
        .load()
    )

    df_stream = df_stream.select(
        col("body").cast("string").alias("decoded_body"),    
        col("partition"),
        col("sequenceNumber"),    
    )

    started_stream = df_stream.writeStream.foreachBatch(write_to_file).trigger(processingTime="5 minutes").start()
    started_stream.awaitTermination()
