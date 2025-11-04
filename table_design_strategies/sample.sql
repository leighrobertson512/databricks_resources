select
  -- salesforce_account_name as `Salesforce Account Name`,
	-- workspace_name as `Workspace Name`,
  region_level_1,
  region_level_2,
  --platform as Platform,
  --sku as SKU,
  cast(case :`Period`
        when 'Day' then daily_date
        when 'Week' then date_trunc('week', daily_date)
        when 'Month' then date_trunc('month', daily_date)
  end as date) as Period,
  product_line,
  sum(
    case :ConsumptionType
      when 'Realized' then dollars
      when 'Customer' then (
        case
          when platform = 'azure' then (dollars/0.85)
          else dollars
        end
      )
    end
  ) as `$DBUs`,
  sum(dbus) as DBUs,
  -- Total columns summing across all product lines for each period/region combination
  sum(sum(
    case :ConsumptionType
      when 'Realized' then dollars
      when 'Customer' then (
        case
          when platform = 'azure' then (dollars/0.85)
          else dollars
        end
      )
    end
  )) over (partition by 
    region_level_1, 
    region_level_2, 
    cast(case :`Period`
      when 'Day' then daily_date
      when 'Week' then date_trunc('week', daily_date)
      when 'Month' then date_trunc('month', daily_date)
    end as date)
  ) as `Total $DBUs`,
  sum(sum(dbus)) over (partition by 
    region_level_1, 
    region_level_2, 
    cast(case :`Period`
      when 'Day' then daily_date
      when 'Week' then date_trunc('week', daily_date)
      when 'Month' then date_trunc('month', daily_date)
    end as date)
  ) as `Total DBUs`
FROM
  main.field_customer_insights.gold_usage_by_workspace
  UNPIVOT (
    (dollars, dbus) -- The columns that will contain the unpivoted values
    FOR product_line      -- The new column that will store the names of the original value groups
    IN (
      -- Interactive
      (notebook_classic_dollars, notebook_classic_dbus) AS notebook_classic, -- Group 1: original columns and their category name
      (notebook_serverless_dollars, notebook_serverless_dbus) AS notebook_serverless,
      -- Lakeflow
      (lakeflow_pipelines_dollars, lakeflow_pipelines_dbus) AS lakeflow_pipelines,
      (lakeflow_connect_saas_cdc_dollars, lakeflow_connect_saas_cdc_dbus) AS lakeflow_connect_saas_cdc,
      (automated_jobs_dollars, automated_jobs_dbus) AS automated_jobs,
      (all_purpose_jobs_dollars, all_purpose_jobs_dbus) AS all_purpose_jobs,
      -- Warehousing
      (dbsql_dollars, dbsql_dbus) AS dbsql,
      -- AI
      (fine_tuning_api_dollars, fine_tuning_api_dbus) AS fine_tuning_api,
      (foundation_model_api_dollars, foundation_model_api_dbus) AS foundation_model_api,
      (ml_serving_serverless_cpu_dollars, ml_serving_serverless_cpu_dbus) AS ml_serving_serverless_cpu,
      (ml_serving_serverless_gpu_dollars, ml_serving_serverless_gpu_dbus) AS ml_serving_serverless_gpu,
      (foundation_model_api_pt_dollars, foundation_model_api_pt_dbus) AS foundation_model_api_pt,
      (ml_serving_classic_dollars, ml_serving_classic_dbus) AS ml_serving_classic,
      (vector_search_dollars, vector_search_dbus) AS vector_search,
      (agent_evaluation_dollars, agent_evaluation_dbus) AS agent_evaluation,
      (apps_dollars, apps_dbus) AS apps,
      (lakehouse_monitoring_dollars, lakehouse_monitoring_dbus) AS lakehouse_monitoring,
      (ai_runtime_dollars, ai_runtime_dbus) AS ai_runtime,
      (batch_inference_dollars, batch_inference_dbus) AS batch_inference,
      (multitenant_provisioned_throughput_dollars, multitenant_provisioned_throughput_dbus) AS multitenant_provisioned_throughput,
      (data_classification_dollars, data_classification_dbus) AS data_classification,
      (ai_gateway_dollars, ai_gateway_dbus) AS ai_gateway,
      -- Lakebase
      (lakebase_dollars, lakebase_dbus) AS lakebase,
      -- MCT
      (mct_dollars, mct_dbus) AS mct,
      -- Others
      (idle_workloads_dollars, idle_workloads_dbus) AS idle_workloads,
      (all_purpose_bi_dollars, all_purpose_bi_dbus) AS all_purpose_bi,
      (unclassified_others_dollars, unclassified_others_dbus) AS unclassified_others
    )
  )
where 1=1
--and salesforce_account_name = :Account 
and dollars != 0
and daily_date >= "2025-01-01"
and daily_date < "2025-10-01"
--and region_level_2 = "EE"
group by all;