-- Genie Agent Options: Genie space ids seen in query history over the last 90 days
-- Source: dashboard "Warehouse & Genie Query Performance" (01f1ce5fa60717a894bf2a26c2a2c7e5), dataset "Genie Agent Options"
-- No parameters. Use the ids returned here for :genie_space_ids in the other queries.

SELECT DISTINCT
  query_source.genie_space_id AS genie_agent
FROM
  system.query.history
WHERE
  workspace_id = '7474654445014356'
  AND query_source.genie_space_id IS NOT NULL
  AND start_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
ORDER BY
  genie_agent;
