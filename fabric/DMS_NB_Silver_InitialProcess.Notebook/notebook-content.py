# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0c61f4ea-76c5-45b7-a741-5505e504e80a",
# META       "default_lakehouse_name": "DMS_LH_Silver",
# META       "default_lakehouse_workspace_id": "e7787afa-5823-4d22-8ca2-af0f38d1a339",
# META       "known_lakehouses": [
# META         {
# META           "id": "0c61f4ea-76c5-45b7-a741-5505e504e80a"
# META         },
# META         {
# META           "id": "d204edd3-9cd3-4399-8d0c-16176355d799"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Imports
# This notebook will read files from bronze lakehouse and map them to silver. Using pre-defined schema and a streaming service.
# 


# CELL ********************

run_as_stream = True
run_test = False
bronze_path = "abfss://e7787afa-5823-4d22-8ca2-af0f38d1a339@onelake.dfs.fabric.microsoft.com/d204edd3-9cd3-4399-8d0c-16176355d799"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging

# Customize the logging format for all loggers
FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(fmt=FORMAT)
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

logger = logging.getLogger('Silver_Initial_Process')
logger.setLevel(logging.DEBUG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run DMS_NB_Shared_SchemaFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name, col, struct, current_timestamp

def apply_schema(df: DataFrame, json_schema_path: str) -> DataFrame:
    """
    Apply a schema to an existing DataFrame given a schema path on OneLake.
    """
    logger.debug(f"Applying schema from: {json_schema_path}")
    schema = open_schema(json_schema_path)
    df_with_schema = spark.createDataFrame(df.rdd, schema)
    logger.debug("Schema applied successfully.")
    return df_with_schema

def restructure_dataframe(df: DataFrame, target_column: str) -> DataFrame:
    """
    Restructure DataFrame by extracting nested fields from a target column.
    """
    logger.debug(f"Restructuring DataFrame, promoting fields from: {target_column}")
    metadata_cols = [col(c) for c in df.columns if c != target_column]
    nested_fields = df.select(f"{target_column}.*").columns
    
    df_transformed = df.select(
        struct(*metadata_cols).alias("MetaData"),
        *[col(f"{target_column}.{field}").alias(field) for field in nested_fields]
    )
    logger.debug("DataFrame restructuring completed.")
    return df_transformed

def persist(df: DataFrame, table_name: str):
    logger.debug(f"Writing to table {table_name}")

    if spark.catalog.tableExists(table_name):
        # If table exists, append data
        write_mode = "append"
    else:
        # If table does not exist, create new table
        write_mode = "overwrite"

    df.write.format("delta") \
        .mode(write_mode) \
        .save(f"Tables/{table_name}")

def transform_bronze(df: DataFrame, target_column: str) -> DataFrame:
    """
    Apply transformation to a bronze DataFrame.
    """
    logger.debug("Transforming bronze DataFrame...")
    df_transformed = restructure_dataframe(df, target_column)
    logger.debug("Bronze transformation completed.")
    return df_transformed

def transform_bronze_and_persist(
    df: DataFrame,
    target_column: str,    
    table_name: str,
    json_schema_path: str
):
    """
    Apply schema, transform bronze DataFrame, and persist it.
    """
    logger.debug("Starting transformation and persistence pipeline...")    
    df = apply_schema(df, json_schema_path)
    df = df.withColumn("SilverCreatedAt", current_timestamp())
    df = restructure_dataframe(df, target_column)
    persist(df, table_name)
    logger.debug("Pipeline completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Stream Data

# CELL ********************

from pyspark.sql.streaming import StreamingQuery

def stop_all_streams():
    active_streams = spark.streams.active
    if not active_streams:
        logger.info("No active streams found.")
        return
    
    for stream in active_streams:
        try:
            logger.info(f"Stopping stream: {stream.name}")
            stream.stop()
        except Exception as e:
            logger.warning(f"Failed to stop stream {stream.name}: {e}")

    logger.info("All active streams have been stopped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import input_file_name, regexp_extract

def load_type_stream(bronze_dms_root_path: str, type: str):
    
    json_schema_path = f"{bronze_dms_root_path}/_meta/bronze_{type}_schema.json"
    schema = open_schema(json_schema_path)

    path = f"{bronze_dms_root_path}/*/*/*/*.json"

    logger.info(f"Starting stream on path {path}")

    return spark.readStream \
        .schema(schema) \
        .option("multiline", "true") \
        .option("maxFilesPerTrigger", 1) \
        .option("badRecordsPath", f"{bronze_path}/Files/dms/{type}/bad_records") \
        .json(path)

def get_file_name(df):
    """Extracts the input file name from a DataFrame (assumes only one row)."""
    files = df.select(input_file_name()).distinct().collect()
    
    if not files:
        return None  # No file name found
    
    return files[0]["input_file_name()"]  # Extracts the first file name

def copy_bad_file(file_path, destination):
    try:
        mssparkutils.fs.mkdirs(destination)  
        mssparkutils.fs.cp(file_path, destination)
        logger.info(f"Moved bad file from {file_path} to {destination}")
    except Exception as e:
        logger.error(f"Failed to move {file_path} to {destination}: {e}")

def on_batch(df, batch_id, type, bronze_dms_root_path, column):
    try:
        if df.isEmpty():
            logger.debug(f"Skipping empty batch {batch_id}")
            return

        logger.debug(f"Batch started {batch_id}")
        
        table_name = f"Test{type.capitalize()}" if run_test else f"{type.capitalize()}"

        json_schema_path = f"{bronze_dms_root_path}/_meta/bronze_{type}_schema.json"
        
        transform_bronze_and_persist(df, column, table_name, json_schema_path)

        file_name = get_file_name(df)
        logger.info(f"Files: {file_name} processed")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")
        
        try:
            file_name = get_file_name(df)
            if not run_test:
                copy_bad_file(file_name, f"{bronze_dms_root_path}/_bad_records")
            else:
                logger.debug("Skipping moving file")
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
            

def process_stream(type: str, column: str):
    
    if run_test:
        bronze_dms_root_path = f"{bronze_path}/Files/test/dms/{type}"
    else:
        bronze_dms_root_path = f"{bronze_path}/Files/dms/{type}"
    
    
    df_stream = load_type_stream(bronze_dms_root_path, type)

    query_writer = df_stream.writeStream \
        .queryName(type) \
        .foreachBatch(lambda df, batch_id: on_batch(df, batch_id, type, bronze_dms_root_path, column))
    
    if not run_test:
        logger.info("Using checkpoint")
        checkpoint_path = f"{bronze_path}/Files/dms/{type}/_checkpoints/"
        query_writer = query_writer.option("checkpointLocation", checkpoint_path)
    else:
        logger.info("Without checkpoint, running once")

    query = query_writer.start()
    return query

if(run_as_stream):
    stop_all_streams()
    query = process_stream("instrument", "Instrument")
    query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
