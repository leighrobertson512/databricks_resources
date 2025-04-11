# Databricks notebook source
def generate_match_insert_columns(match_columns, dataframe):
    """
    Get a list of column names from a PySpark DataFrame.

    Args:
        match_columns(str): list of the columns to match on
        dataframe (DataFrame): The PySpark DataFrame.

    Returns:
        list: A list of column names.
    """
    

    match_columns_list = match_columns.split(", ")
    all_columns = dataframe.columns
    update_list = [x for x in all_columns if x not in match_columns_list]
    
    return match_columns_list, update_list, all_columns

# COMMAND ----------

#this would go in a utilities notebook 
def dynamic_merge_sql(table_name, match_columns, update_columns, insert_columns):
    """
    Build dynamic merge statement for use when merging with any code 

    Args:
        table_name (str): The name of the Delta Lake table.
        match_columns (list): A list of the columns to match on aka the tables primary/composite key
        update_columns (list): Columns which will be updated when a record matches 
        insert_columns (list): All columns in the table for the insert part where records don't match

    Returns:
        str: A string with the merge statement for this table
    """

    # Create the "USING" clause for matching columns
    using_clause = "USING updates AS updates ON "
    using_clause += " AND ".join([f"target.{col} = updates.{col}" for col in match_columns])

    # Create the "WHEN MATCHED" clause for updating columns
    update_clause = "WHEN MATCHED THEN UPDATE SET "
    update_clause += ", ".join([f"target.{col} = updates.{col}" for col in update_columns])

    # Create the "WHEN NOT MATCHED" clause for inserting all columns
    insert_clause = "WHEN NOT MATCHED THEN INSERT ("
    insert_clause += ", ".join(insert_columns) + ") VALUES ("
    insert_clause += ", ".join([f"updates.{col}" for col in insert_columns]) + ")"

    # Assemble the complete MERGE statement
    merge_sql = f"""
    MERGE INTO {table_name} AS target
    {using_clause}
    {update_clause}
    {insert_clause}
    """

    return merge_sql

# COMMAND ----------

from pyspark.sql import DataFrame

def generate_ddl(df: DataFrame, catalog: str, schema: str, table_name: str) -> str:
    """
    Generate a DDL statement for creating a table in Databricks.

    Parameters:
    - df: PySpark DataFrame
    - catalog: Catalog name
    - schema: Schema name
    - table_name: Table name

    Returns:
    - DDL statement as a string
    """
    
    # Get the schema of the DataFrame
    schema_fields = df.schema.fields
    
    # Generate column definitions
    columns = []
    for field in schema_fields:
        column_name = field.name
        data_type = field.dataType
        
        if isinstance(data_type, type) and hasattr(data_type, 'simpleString'):
            data_type_str = data_type.simpleString()
            
            # Handle nested types (e.g., StructType, ArrayType)
            if data_type_str.startswith("StructType"):
                # Use VARIANT for StructType
                data_type_str = "VARIANT"
            elif data_type_str.startswith("ArrayType"):
                # Use VARIANT for ArrayType
                data_type_str = "VARIANT"
            
            columns.append(f"{column_name} {data_type_str}")
        else:
            # If the type is not recognized, default to VARIANT
            columns.append(f"{column_name} VARIANT")
    
    # Construct the DDL statement with proper new lines
    ddl = f"""
USE CATALOG {catalog};
USE SCHEMA {schema};

CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
"""
    
    # Add each column definition on a new line
    for column in columns:
        ddl += f"    {column},"
    
    # Remove the trailing comma and add the closing parenthesis
    ddl = ddl.rstrip(",\n") + "\n);"
    
    return ddl




# COMMAND ----------


