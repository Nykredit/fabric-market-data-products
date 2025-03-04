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
# META         },
# META         {
# META           "id": "0c61f4ea-76c5-45b7-a741-5505e504e80a"
# META         },
# META         {
# META           "id": "67b6ec45-f491-492b-91f0-1d3b8bb24631"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Status
# This notebook is generally just used to look at the status of individual items in bronze, silver and gold. To sanity check that data integrity is correct.

# CELL ********************

bronze_path = "abfss://e7787afa-5823-4d22-8ca2-af0f38d1a339@onelake.dfs.fabric.microsoft.com/d204edd3-9cd3-4399-8d0c-16176355d799"

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
parent_folder = "Files/dms/instrument/2025/*/*/*.json"  # Update with your actual path

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

df = spark.sql("""
    SELECT 
        CAST(MetaData.BronzeCreatedAt AS DATE) AS BronzeCreatedDate, 
        COUNT(*) AS RecordCount
    FROM DMS_LH_Silver.Instrument
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

df = spark.sql("""
    SELECT 
        GainID,
        COUNT(*) AS RecordCount
    FROM DMS_LH_Silver.Instrument
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

df = spark.sql("""
    SELECT 
        CAST(MetaData_BronzeCreatedAt AS DATE) AS BronzeCreatedDate, 
        COUNT(*) AS RecordCount
    FROM DMS_LH_Gold.Instrument
    GROUP BY CAST(MetaData_BronzeCreatedAt AS DATE)
    ORDER BY BronzeCreatedDate
""")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
