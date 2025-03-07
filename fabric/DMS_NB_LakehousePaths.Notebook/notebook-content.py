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

# CELL ********************

class LakehouseUtils:
    dev_workspace_id = "e7787afa-5823-4d22-8ca2-af0f38d1a339"
    bronze_lakehouse = "DMS_LH_Bronze"
    silver_lakehouse = "DMS_LH_Silver"
    gold_lakehouse = "DMS_LH_Gold"

    @staticmethod
    def get_lakehouse_abs_path(lakehouse_name, workspace_id=''):
        return mssparkutils.lakehouse.get(lakehouse_name, workspace_id).get("properties").get("abfsPath")

    @staticmethod
    def get_local_bronze_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.bronze_lakehouse)

    @staticmethod
    def get_local_silver_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.silver_lakehouse)

    @staticmethod
    def get_local_gold_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.gold_lakehouse)

    @staticmethod
    def get_dev_bronze_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.bronze_lakehouse, LakehouseUtils.dev_workspace_id)

    @staticmethod
    def get_dev_silver_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.silver_lakehouse, LakehouseUtils.dev_workspace_id)

    @staticmethod
    def get_dev_gold_path():
        return LakehouseUtils.get_lakehouse_abs_path(LakehouseUtils.gold_lakehouse, LakehouseUtils.dev_workspace_id)
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
