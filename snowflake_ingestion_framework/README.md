# Snowflake Ingestion Framework

A reusable framework for loading data from Snowflake into Databricks Delta tables with liquid clustering.

## Folder Structure

| File | Purpose |
| --- | --- |
| `meta_snowflake_framework` | Main orchestrator notebook. Reads from Snowflake, writes/merges into Delta with liquid clustering. |
| `utils_file` | Shared utility functions (Snowflake connector, dynamic merge, vacuum, validation). |
| `obs_tables_config_json` | Table configuration JSON defining source/target mappings and clustering keys. |

## Prerequisites

1. **Databricks Secret Scope** — A scope (default name: `snowflake_credentials`) must exist with these keys:
   - `host` — Snowflake account URL (e.g. `account.snowflakecomputing.com`)
   - `user` — Snowflake username
   - `password` — Snowflake password
   - `warehouse` — Snowflake warehouse name
   - `role` — Snowflake role

2. **Target catalog/schema** must already exist in Unity Catalog.

## Configuration (obs_tables_config_json)

Each key in `table_json` represents a source table. Required fields:

```python
"table_name": {
    "snowflake_database": "MY_DB",          # Snowflake database
    "snowflake_schema": "MY_SCHEMA",        # Snowflake schema
    "source_incremental_column": "updated_at",  # Column used for incremental filtering
    "target_catalog": "my_catalog",         # Databricks target catalog
    "target_schema": "my_schema",           # Databricks target schema
    "target_table": "my_table",             # Databricks target table name
    "target_merge_columns": ["id"],         # Primary/composite key for MERGE
    "target_incremental_column": "updated_at",  # Target column for watermark
    "cluster_by": ["id", "date_col"]        # Optional: liquid clustering columns (max 4)
}
```

### Clustering Rules

- `cluster_by` accepts a list of **1 to 4** column names.
- If omitted or set to `["auto"]`, the table uses automatic liquid clustering (`CLUSTER BY AUTO`).
- If more than 4 columns are provided, the framework raises a `ValueError`.

## Job Widgets (Runtime Parameters)

| Widget | Default | Description |
| --- | --- | --- |
| `source_table` | `customer_fact` | Key from `table_json` identifying which table to load. |
| `incremental` | `True` | `True` for incremental merge, `False` for full reload. |
| `date_cutoff` | (empty) | Optional date string to replay data from a specific point forward. Overrides incremental watermark. |

## How It Works

### Full Load (`incremental = False`)
1. Reads the entire source table from Snowflake.
2. Drops the existing Delta target table (if any).
3. Creates a new Delta table with `CLUSTER BY` (explicit columns or AUTO).
4. Vacuums the table.

### Incremental Load (`incremental = True`)
1. Reads the max value of `target_incremental_column` from the existing Delta target.
2. Pulls only records from Snowflake where `source_incremental_column >= max_date`.
3. Merges into the target using a dynamic MERGE statement (match on `target_merge_columns`).
4. Vacuums the table.

### Date Cutoff Override
If `date_cutoff` is provided, it overrides both full and incremental logic — filtering the source from that date forward regardless of the `incremental` flag.

## Modifying the Secret Scope Name

The scope name is defined as a variable at the top of `utils_file`:

```python
SNOWFLAKE_SECRET_SCOPE = "snowflake_credentials"
```

Change this single variable to point to a different scope.

## Scheduling as a Job

This notebook is designed to be parameterized via widgets and run as a Databricks Job task. To ingest multiple tables, create a job with one task per table, each passing a different `source_table` widget value.

## Notes

- No manual `OPTIMIZE` or `ZORDER` is needed — liquid clustering handles compaction automatically.
- The Snowflake connector pushes filters down to Snowflake when possible, but if the read is slow, optimize on the Snowflake side (e.g., clustering the source table on the incremental column).
- The `VACUUM` retention is set to 160 hours by default. Adjust as an organization-wide standard.
