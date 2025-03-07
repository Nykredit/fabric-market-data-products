# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run DMS_NB_LakehousePaths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mounting the dev and local bronze lakehouses
local_bronze_root = LakehouseUtils.get_local_bronze_path()
dev_bronze_root = LakehouseUtils.get_dev_bronze_path()

def copy_bronze_data(subfolder):
    """
    Copies files from a specific subfolder in the dev bronze lakehouse
    to the same subfolder in the local bronze lakehouse.
    
    :param subfolder: The subfolder path to copy (relative to the bronze root)
    """    
    dev_bronze_path = f"{dev_bronze_root}/Files/{subfolder.strip('/')}"
    local_bronze_path = f"{local_bronze_root}/Files/{subfolder.strip('/')}"
    
    try:
        files_in_dev_bronze = mssparkutils.fs.ls(dev_bronze_path)
    except Exception as e:
        print(f"Error: Source subfolder does not exist - {dev_bronze_path}")
        return
        
    mssparkutils.fs.mkdirs(local_bronze_path)
        
    for file_info in files_in_dev_bronze:
        source = file_info.path
        destination = source.replace(dev_bronze_root, local_bronze_root)

        if file_info.isDir:
            print(f"Skipping directory: {source}")
        else:
            mssparkutils.fs.cp(source, destination)
            print(f"Copied {source} to {destination}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

copy_bronze_data("_meta")
copy_bronze_data("/dms/instrument/_meta/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
