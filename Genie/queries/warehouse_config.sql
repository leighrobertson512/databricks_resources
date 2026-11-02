-- Warehouse Config: latest configuration for each SQL warehouse in the workspace
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Warehouse Config"
-- No parameters.

SELECT
  warehouse_id,
  warehouse_name,
  warehouse_type,
  warehouse_size,
  warehouse_channel,
  min_clusters,
  max_clusters,
  auto_stop_minutes,
  CASE
    WHEN delete_time IS NOT NULL THEN 'Deleted'
    ELSE 'Active'
  END AS status,
  change_time
FROM
  system.compute.warehouses
WHERE
  workspace_id = '7474654445014356'
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) = 1;
