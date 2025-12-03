import dlt
from pyspark.sql.functions import *

# The user requested 'create_auot_cdc_from_snapshot_flow' which is likely 'create_auto_cdc_from_snapshot_flow'.
# This API handles CDC from a snapshot source to an SCD2 target.

# Define the source table
source_table_name = "leigh_robertson_demo.silver_noaa.local_snapshot"

# Create the target streaming table
dlt.create_streaming_table(
    name="local_snapshot_scd2",
    comment="SCD Type 2 table from local snapshot",
    table_properties={"quality": "gold"}
)

# Apply the CDC flow
dlt.create_auto_cdc_from_snapshot_flow(
    target="local_snapshot_scd2",
    source=source_table_name,
    keys=["post_code", "startTime"],  # Assumed keys based on other NOAA tables; update if different
    stored_as_scd_type=2
)

