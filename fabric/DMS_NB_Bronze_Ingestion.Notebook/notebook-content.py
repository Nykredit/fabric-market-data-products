# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d204edd3-9cd3-4399-8d0c-16176355d799",
# META       "default_lakehouse_name": "DMS_LH_Bronze",
# META       "default_lakehouse_workspace_id": "e7787afa-5823-4d22-8ca2-af0f38d1a339",
# META       "known_lakehouses": [
# META         {
# META           "id": "d204edd3-9cd3-4399-8d0c-16176355d799"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "345cc1d2-5570-8779-4fbc-a5ab9a8a1543",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Ingestion

# CELL ********************

run_test = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime as dt
import pytz
import json
import logging

with open("/lakehouse/default/Files/_meta/EventHubConnection.txt", "r") as file:
    sharedKey = file.readline()

endpoint = "sb://nywt-dms-tst-function-eventhub.servicebus.windows.net/"
event_hub_name = "nywt-dms-tst-function-dms"

event_hub_connection_string = f"Endpoint={endpoint};SharedAccessKeyName=ListenSharedAccessKey;SharedAccessKey={sharedKey};EntityPath={event_hub_name}"


# Start from beginning of stream
startOffset = "-1"

# End at the current time. This datetime formatting creates the correct string format from a python datetime object
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            
  "enqueuedTime": None,   
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,           
  "seqNo": -1,
  "enqueuedTime": endTime,
  "isInclusive": True
}


eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string),    
}


# Put the positions into the Event Hub config dictionary
eh_conf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
eh_conf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

# Function to stop existing streams
def stop_existing_streams():
    for stream in spark.streams.active:
        print(f"Stopping stream: {stream.id}")
        stream.stop()

# Stop any existing streams before starting a new one
stop_existing_streams()

output_base_path = "Files/test/dms/" if run_test else "Files/dms/"

def write_to_file(df, epoch_id):
    if df.isEmpty():
        print(f"No data received in batch {epoch_id}. Skipping save.")
        return

    # Convert DataFrame to Pandas for row-wise iteration
    pdf = df.select("decoded_body", "sequenceNumber", "partition").toPandas()

    for idx, row in pdf.iterrows():
        # Generate partitioned path based on current UTC date
        
        now = dt.now(tz=pytz.utc)
        json_content = json.loads(row["decoded_body"])
        json_content["BronzeCreatedAt"] = now.isoformat()

        event_type = json_content.get("Type", "unknown").lower()

        partition_path = f"{output_base_path}/{event_type}/{now.year}/{now.month:02d}/{now.day:02d}/"

        # Unique filename per event
        sequence_number = row["sequenceNumber"]
        partition = row["partition"]
        output_path = f"{partition_path}event_{partition}_{sequence_number}.json"

        # Convert decoded_body to JSON string and write to Fabric Lakehouse
        json_data = json.dumps(json_content)
        notebookutils.fs.put(output_path, json_data, overwrite=True)

        print(f"Saved event {idx + 1} to {output_path}")


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

started_stream = df_stream.writeStream.foreachBatch(write_to_file).start()

started_stream.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
