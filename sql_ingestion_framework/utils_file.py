# Databricks notebook source
# MAGIC %md
# MAGIC This file will be used to store common functions used by the entire. No non standarad libraries should be imported as it can cause issues for other people as their notebooks will fail unless they pip install the non standard library
# MAGIC

# COMMAND ----------

def get_source_data(connection, catalog, database, table_name):
    """
    Returns a dataframe with the data from a given SQL server

    Args:
        connection (str): The name of the sql dbx secret where credentials are contained
        catalog (str): SQL catalog name
        database (str): SQL database name
        table_name (str): SQL table name

    Returns:
        Dataframe: A pyspark dataframe with the source data
    """

    #sql_credentials = 'temp_sqlserver_merp' 
    sql_credentials = connection.lower()
    jdbcDatabase = database.lower()
    jdbcHostname = dbutils.secrets.get(sql_credentials,'hostname')
    jdbcPort = dbutils.secrets.get(sql_credentials,'port')
    username = dbutils.secrets.get(sql_credentials,'username')
    password = dbutils.secrets.get(sql_credentials,'password')

    jdbcUrl = 'jdbc:sqlserver://{0}:{1};database={2};trustServerCertificate=true;'.format(jdbcHostname, jdbcPort, jdbcDatabase)
    connectionProperties = {
                            'user': username,
                            'password': password,
                            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
                           }
    df = spark.read.jdbc(url=jdbcUrl, table= table_name, properties=connectionProperties)

    return df 

# COMMAND ----------

def get_source_data_demo(catalog, schema, table_name):
    """
    Returns a dataframe with the data from a given SQL server

    Args:
        catalog (str): SQL catalog name
        database (str): SQL database name
        table_name (str): SQL table name

    Returns:
        Dataframe: A pyspark dataframe with the source data
    """

    #sql_credentials = 'temp_sqlserver_merp' 
    df = spark.table(f'{catalog}.{schema}.{table_name}')

    return df 

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

def vacuum_table(table_name, retain_hours):
    """
    Vacuum a Databricks Delta Lake table to remove old data files.

    Args:
        table_name (str): The name of the Delta Lake table.
        retain_hours (int): The number of hours to retain data files.

    Returns:
        str: A message indicating the result of the VACUUM operation.
    """
    try:
        # Construct the VACUUM SQL command
        vacuum_sql = f"VACUUM {table_name} RETAIN {retain_hours} HOURS"

        # Execute the VACUUM command
        spark.sql(vacuum_sql)

        # Return a success message
        return f"VACUUM operation on table '{table_name}' completed successfully."

    except Exception as e:
        # Handle any errors and return an error message
        return f"Error performing VACUUM operation on table '{table_name}': {str(e)}"

# COMMAND ----------

from pyspark.sql import DataFrame

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


