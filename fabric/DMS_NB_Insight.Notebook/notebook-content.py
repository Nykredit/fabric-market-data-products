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

# # Status
# This notebook is generally just used to look at the status of individual items in bronze, silver and gold. To sanity check that data integrity is correct.

# CELL ********************

%run DMS_NB_LakehousePaths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_path = LakehouseUtils.get_lakehouse_path_by_keyword("bronze")
silver_path = LakehouseUtils.get_lakehouse_path_by_keyword("silver")
gold_path = LakehouseUtils.get_lakehouse_path_by_keyword("gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bronze Folder

# CELL ********************

from pyspark.sql.functions import input_file_name, regexp_replace

# Define the parent folder path in the lakehouse
parent_folder = f"{bronze_path}/Files/dms/instrument/2025/*/*/*.json"  # Update with your actual path

# List all files in the folder and subfolders
files_df = spark.read.format("binaryFile").load(parent_folder + "**")

# Extract full file path
files_df = files_df.withColumn("full_path", input_file_name())

files_df = files_df.withColumn("folder", regexp_replace(files_df["full_path"], "/[^/]+$", ""))
files_df = files_df.withColumn("folder", regexp_replace(files_df["folder"], bronze_path, ""))    

# Count files per folder
folder_counts = files_df.groupBy("folder").count()

# Show the number of files per unique folder
folder_counts.show(truncate=False)

# Print the total number of files
total_files = files_df.count()
print(f"Total number of files: {total_files}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver Table

# CELL ********************

df = spark.read.format("delta").load(f"{silver_path}/Tables/Instrument")
df.createOrReplaceTempView("Instrument")

df = spark.sql("""
    SELECT 
        CAST(MetaData.BronzeCreatedAt AS DATE) AS BronzeCreatedDate, 
        COUNT(*) AS RecordCount
    FROM Instrument
    GROUP BY CAST(MetaData.BronzeCreatedAt AS DATE)    
    ORDER BY BronzeCreatedDate
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Duplicate
# This function is to show, how silver record often comes in multiple times for the same GainID

# CELL ********************

df = spark.read.format("delta").load(f"{silver_path}/Tables/Instrument")
df.createOrReplaceTempView("Instrument")


df = spark.sql("""
    SELECT 
        GainID,
        COUNT(*) AS RecordCount
    FROM Instrument
    WHERE CAST(MetaData.BronzeCreatedAt AS DATE)  = "2025-03-04"
    GROUP BY GainID    
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold Table

# CELL ********************

df = spark.read.format("delta").load(f"{gold_path}/Tables/Instrument")
df.createOrReplaceTempView("Instrument")

df = spark.sql("""
    SELECT 
        CAST(MetaData_BronzeCreatedAt AS DATE) AS BronzeCreatedDate, 
        COUNT(*) AS RecordCount
    FROM Instrument
    GROUP BY CAST(MetaData_BronzeCreatedAt AS DATE)
    ORDER BY BronzeCreatedDate
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
