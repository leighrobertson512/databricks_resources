from pyspark.sql.functions import current_timestamp

def add_audit_columns(df):
    return df.withColumn("audit_update_ts", current_timestamp())