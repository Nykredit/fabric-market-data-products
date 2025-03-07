# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Transformation
# This Notebook runs in streaming mode on the silver delta lake table and performs the following tasks:
# 
# **Stream Handling**:
# - Processes the stream in micro batches.
# - Flattens nested objects.
# - Splits arrays into their own tables with a reference to the main table.
# 
# **Table Structures:**
# - Main table contains one row per input row from the silver layer.
# - Derived flattened array tables can have multiple rows with the same ID.
# - A unique ID `MetaData_GoldUniqueId` is created for each row in the derived table to perform merges.
# - Adds a timestamp column `MetaData_GoldCreatedAt` indicating when the query is initiated.
# 
# Main table is names as `Main` and derived tables are named as `Main_ArrayName`
# 
# **Data Merging:**
# - When a data row with the same identifier as in the silver layer is already loaded in the gold main table, the data is merged.
# - Updates are applied in the main table.
# - All rows in the derived tables are replaced with the new updated data.
# - Each table is handled in a single merge operation to ensure consistency on the delta lake table.
# - In case of multiple rows in silver is streamed in with the same identifier, the `MetaData_PublicationDate` column
#   is used to determine which one is applied on the data. The latest is applied.


# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode, monotonically_increasing_id, current_timestamp, lit, concat, row_number
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable

import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run DMS_NB_LakehousePaths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constants & Functions

# CELL ********************

# OBS: Only use in Spark Definition Job
# # Spark session builder, appName appears in log entries
# spark = (SparkSession.builder.appName("DMS_SJ_Gold_Transformation").getOrCreate())
# spark_context = spark.sparkContext

# # Setup logging
# log4jLogger = spark_context._jvm.org.apache.log4j
# logger = log4jLogger.LogManager.getLogger(spark.conf.get("spark.app.name"))
# logger.setLevel(log4jLogger.Level.INFO)

# Only use in notebooks
# Setup logging
logger = logging.getLogger('DMS_SJ_Gold_Transformation')
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paths in OneLake

silver_path = LakehouseUtils.get_local_silver_path()
gold_path = LakehouseUtils.get_local_gold_path()

run_as_stream = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def expand_struct_column(df: DataFrame, struct_col: str, alias_prefix: str = "") -> DataFrame:
    """
    Expands a StructType column by extracting its fields and adding them as separate columns.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    struct_col : str
        Name of the StructType column to expand.
    alias_prefix : str, optional
        Prefix to add to expanded column names to prevent conflicts.

    Returns
    -------
    DataFrame
        Updated DataFrame with expanded struct fields.
    """
    logger.debug(f"Expanding struct column: {struct_col}")

    struct_fields = df.schema[struct_col].dataType.fields  # Get schema of the struct field

    # Generate new column expressions for each nested field
    expanded_cols = [col(f"{struct_col}.{field.name}").alias(f"{alias_prefix}{field.name}") for field in struct_fields]

    # Select all original columns (except the struct column) and add expanded fields
    df = df.select(*(col(c) for c in df.columns if c != struct_col), *expanded_cols)

    return df


def flatten_struct_columns(df: DataFrame) -> DataFrame:
    """
    Recursively flattens all StructType columns in the DataFrame.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        Flattened DataFrame with struct fields extracted as separate columns.
    """
    struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]

    while struct_columns:
        for struct_col in struct_columns:
            df = expand_struct_column(df, struct_col, alias_prefix=f"{struct_col}_")  # Add prefix for clarity

        # Check if there are any remaining StructType columns
        struct_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StructType)]

    return df


def remove_array_columns(df: DataFrame) -> DataFrame:
    """
    Removes all ArrayType columns from the DataFrame.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.

    Returns
    -------
    DataFrame
        Updated DataFrame without ArrayType columns.
    """
    array_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]
    
    if array_columns:
        logger.debug(f"Removing array columns: {array_columns}")

    return df.select(*[col.name for col in df.schema.fields if col.name not in array_columns])


def merge_derived_table(new_df: DataFrame, delta_table_path: str, id_column: str):
    """
    Merge a new DataFrame into an existing Delta table, ensuring unique identifiers
    and handling updates. If the Delta table does not exist, it will be created.
    An auto-generated unique identifier is added to each row in the new DataFrame.

    Parameters
    ----------
    new_df : DataFrame
        The new DataFrame to be merged into the Delta table.
    delta_table_path : str
        The path to the Delta table.
    id_column : str
        Column used as a reference ID. The column name used to identify matching rows between the
        new DataFrame and the existing Delta table.
    """

    # Add a autogenerated unique identifier to the DataFrame
    new_df = new_df.withColumn(
        "MetaData_GoldUniqueId",
        concat(monotonically_increasing_id(), lit("_"), current_timestamp())
    )

    # Create delta table and return if first time
    if not DeltaTable.isDeltaTable(spark, delta_table_path):
        new_df.write.format("delta").mode("append").save(delta_table_path)
        return

    # Identify rows to update and union the new rows
    current_df = spark.read.format("delta").load(delta_table_path)
    overlapping_ids = (
        current_df.join(new_df, getattr(current_df, id_column) == getattr(new_df, id_column), how="inner")
        .select(*[getattr(current_df, col) for col in current_df.columns])
    )
    union_df = overlapping_ids.union(new_df)

    # Perform the merge operation where old rows are deleted and new are inserted
    target_table = DeltaTable.forPath(spark, delta_table_path)
    target_table.alias("target").merge(
        union_df.alias("source"),
        "target.MetaData_GoldUniqueId = source.MetaData_GoldUniqueId"
    ).whenMatchedDelete().whenNotMatchedInsertAll().execute()


def split_and_write_array_columns(
    df: DataFrame,
    base_table_name: str,
    id_column: str
):
    """
    Extracts all ArrayType columns, explodes them, and writes each array into its own table.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    base_table_name : str
        Base name for output tables.
    id_column : str
        Column used as a reference ID.
    -------
    """
    array_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)]

    for array_col in array_columns:
        logger.debug(f"Processing array column: {array_col}")
        exploded_df = df.select(id_column, explode(col(array_col)).alias(array_col))
        
        # Rename the id_column to have the base_table_name as prefix, in case of column name collisions
        prefixed_id_column = f"{base_table_name}_{id_column}"
        exploded_df = exploded_df.withColumnRenamed(id_column, prefixed_id_column)

        # Flatten struct fields within the exploded array, if applicable.
        if isinstance(df.schema[array_col].dataType.elementType, StructType):
            exploded_df = expand_struct_column(exploded_df, array_col)

        table_path = f"{gold_path}/Tables/{base_table_name}_{array_col}"
        logger.debug(f"Merging exploded array data to table: {table_path}")
        merge_derived_table(exploded_df, table_path, prefixed_id_column)


def flatten_and_process_data(
    df: DataFrame,
    table_name: str,
    id_column: str
):
    """
    Cleans, flattens, and processes a DataFrame:
    - Drops unnecessary metadata.
    - Flattens nested StructType columns.
    - Splits and writes ArrayType columns into separate tables.
    - Writes the cleaned main DataFrame back to the lakehouse.
    
    Disclaimer
    ----------
    Does not work on nested StructTypes or ArrayTypes inside of the ArrayType
    as flattening is done before splits. This function could be improved to be recursive,
    if the need arises.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    table_name : str
        Target table name.
    id_column : str
        Column used as a reference ID.
    """
    logger.debug("Starting data transformation process...")

    # Drop rows where GainID is Null
    df.filter(getattr(df, id_column).isNotNull())

    # Handle multiple updates to the same identifier in same dataframe, take the latest by PublicationDate.
    window_spec = Window.partitionBy(id_column).orderBy(col("MetaData.PublicationDate").desc())
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    single_id_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

    # Flatten all StructType columns
    logger.debug("Flattening struct columns...")
    flattened_df = flatten_struct_columns(single_id_df)

    # Process and write array columns to their own tables
    logger.debug("Processing array columns...")
    split_and_write_array_columns(flattened_df, table_name, id_column)

    # Remove the array columns from the main table and add timestamp
    main_df = remove_array_columns(flattened_df)    
    main_df = main_df.withColumn("MetaData_GoldCreatedAt", current_timestamp())
    
    logger.debug(f"Merging the cleaned data to main table: {table_name}")
    main_table_path = f"{gold_path}/Tables/{table_name}"

    if not DeltaTable.isDeltaTable(spark, main_table_path):
        # Create delta table and return if first time
        main_df.write.format("delta").mode("append").save(main_table_path)
    else:
        # Merge the cleaned DataFrame
        target_table = DeltaTable.forPath(spark, main_table_path)
        target_table.alias("target").merge(
            main_df.alias("source"),
            f"target.{id_column} = source.{id_column}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.debug("Data transformation process completed successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## Stream Data

# CELL ********************

def on_batch(
    micro_batch_df: DataFrame,
    batch_id: int,
    source_name: str,
    source_id_col: str
):
    """
    Processes each micro-batch of data.

    Parameters
    ----------
    micro_batch_df : DataFrame
        The micro-batch DataFrame to process.
    batch_id : int
        The unique identifier for the batch.
    source_name : str
        The name of the data source.
    source_id_col : str
        The idenifier for the source table.
    """
    try:
        logger.info(f"Starting batch {batch_id}.")
        if micro_batch_df.isEmpty():
            logger.info(f"Skipping empty batch {batch_id}.")
            return

        flatten_and_process_data(micro_batch_df, source_name, source_id_col)
        logger.info(f"Persisted {micro_batch_df.count()} items from stream")

    except Exception as e:
        # TODO: Write failure DataFrames or IDs to somewhere
        logger.error(f"Error processing batch {batch_id}: {e}")


def process_stream(source_name: str, source_id_col: str) -> StreamingQuery:
    """
    Sets up and starts the stream processing.

    Parameters
    ----------
    source_name : str
        The name of the data source.
    source_id_col : str
        The idenifier for the source table.

    Returns
    -------
    StreamingQuery
        The streaming query object.
    """
    # Delta table in silver is used as stream source
    df_stream = spark.readStream.format("delta").load(f"{silver_path}/Tables/{source_name}")

    # Create checkpoint, apply micro batch function, and start stream processing
    checkpoint_path = f"{silver_path}/Files/dms/{source_name}/_checkpoints/"
    query = (
        df_stream.writeStream
        .foreachBatch(lambda df, batch_id: on_batch(df, batch_id, source_name, source_id_col))
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
    return query


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = process_stream(source_name="Instrument", source_id_col="GainID")
query.awaitTermination()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
