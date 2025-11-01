-- Monitoring model for news freshness and data quality (run with: dbt run --select tag:monitoring)
{{ config(
    materialized='table', 
    tags=['monitoring']
) }}

WITH latest_runs AS (
  SELECT
    MAX(web_publication_date) AS latest_pub_date,
    COALESCE(MAX(DBT_UPDATED_AT), CURRENT_TIMESTAMP()) AS last_dbt_run,  -- Uppercase + COALESCE fallback if missing
    COUNT(DISTINCT article_id) AS recent_articles,
    CURRENT_TIMESTAMP() AS check_timestamp,
    -- New: Track row drops for quality gating
    (SELECT COUNT(*) FROM {{ ref('stg_sf__guardian') }}) AS staging_total,
    COUNT(DISTINCT article_id) AS fact_total  -- Reuse recent_articles as proxy; adjust if needed for full count
  FROM {{ ref('fct_articles') }}
  WHERE web_publication_date >= DATEADD('day', -7, CURRENT_DATE())  -- Last week
)

SELECT
  latest_pub_date,
  last_dbt_run,
  DATEDIFF('hour', latest_pub_date, check_timestamp) AS freshness_hours,
  recent_articles,
  CASE
    WHEN DATEDIFF('hour', latest_pub_date, check_timestamp) <= 24 THEN 'Fresh'
    WHEN DATEDIFF('hour', latest_pub_date, check_timestamp) <= 72 THEN 'Moderate'
    ELSE 'Stale - Alert!'
  END AS freshness_status,
  -- New: Data quality metrics
  staging_total,
  fact_total,
  staging_total - fact_total AS row_drop_count,
  ROUND((fact_total * 100.0 / NULLIF(staging_total, 0)), 2) AS retention_rate_percent,
  CASE
    WHEN (staging_total - fact_total) > 50 THEN 'High Drop - Review Filters'
    WHEN (staging_total - fact_total) > 0 THEN 'Minor Drop - Expected'
    ELSE 'Full Retention'
  END AS drop_status,
  check_timestamp
FROM latest_runs
ORDER BY check_timestamp DESC
LIMIT 1
