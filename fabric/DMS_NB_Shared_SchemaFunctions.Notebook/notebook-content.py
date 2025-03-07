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

# # Shared schema functionality
# This is general helper functionality for loading and saving schemas in DMS.

# MARKDOWN ********************

# ## Imports

# CELL ********************

from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions

# CELL ********************

def open_schema(schema_path: str) -> StructType:
    """
    Load a schema from a JSON file and return it as a StructType.

    Parameters
    ----------
    schema_path : str
        The path to the schema JSON file.

    Returns
    -------
    StructType
        The schema loaded from the JSON file.
    """
    schema_json = notebookutils.fs.head(schema_path)
    return StructType.fromJson(json.loads(schema_json))


def write_schema_from_df(df: DataFrame, schema_path: str):
    """
    Write the schema of a DataFrame to a JSON file.

    Parameters
    ----------
    df : DataFrame
        The DataFrame whose schema is to be written.
    schema_path : str
        The full path including filename and '.json' for where
        the schema JSON file will be saved.
    """
    # Create directories if they do not already exist
    file_directory = "/".join(schema_path.split("/")[:-1])
    if not notebookutils.fs.exists(file_directory):
        notebookutils.fs.mkdirs(file_directory)

    notebookutils.fs.put(schema_path, df.schema.json(), True)


def infer_schema_from_json_files_and_write_to_file(
    json_folder_path:str,
    schema_path: str
):
    """
    Infer the schema from JSON files in a folder and write it to a JSON file.

    Parameters
    ----------
    json_folder_path : str
        The path to the folder containing JSON files.
    schema_path : str
        The full path including filename and '.json' for where
        the inferred schema JSON file will be saved.
    """
    df_with_super_schema = spark.read.json(json_folder_path)
    write_schema_from_df(df_with_super_schema, schema_path)


def validate_schema(df: DataFrame, schema_path: str) -> bool:
    """
    Validate the schema of a DataFrame against a schema from a
    JSON file and print differences in case they do not match.

    Parameters
    ----------
    df : DataFrame
        The DataFrame whose schema is to be validated.
    schema_path : str
        The path to the schema JSON file.

    Returns
    -------
    bool
        True if the DataFrame schema matches the schema from the
        JSON file, False otherwise.
    """
    schema_from_file = open_schema(schema_path)
    df_schema = df.schema

    differences = []

    # Check for missing or extra columns
    df_fields = set(df_schema.fieldNames())
    file_fields = set(schema_from_file.fieldNames())

    missing_columns = file_fields - df_fields
    extra_columns = df_fields - file_fields

    if missing_columns:
        differences.append(f"Missing columns: {missing_columns}")
    if extra_columns:
        differences.append(f"Extra columns: {extra_columns}")

    # Check for differences in types, nullability, and metadata
    for schema_field in schema_from_file:
        if schema_field.name in df_fields:
            df_field = df_schema[schema_field.name]
            if schema_field.dataType != df_field.dataType:
                differences.append(f"Column '{schema_field.name}' has different types: {schema_field.dataType} (expected) vs {schema_field.dataType} (actual)")
            if schema_field.nullable != df_field.nullable:
                differences.append(f"Column '{schema_field.name}' has different nullability: {schema_field.nullable} (expected) vs {schema_field.nullable} (actual)")
            if schema_field.metadata != df_field.metadata:
                differences.append(f"Column '{schema_field.name}' has different metadata: {schema_field.metadata} (expected) vs {schema_field.metadata} (actual)")

    # Check for column order differences
    if list(schema_from_file.fieldNames()) != list(df_schema.fieldNames()):
        differences.append("Column order is different")

    if differences:
        print("Schema differences found:")
        for difference in differences:
            print(difference)
        return False
    else:
        print("Schemas match")
        return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
