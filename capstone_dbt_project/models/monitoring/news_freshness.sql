-- Monitoring model for news freshness and data quality (run with: dbt run --select tag:monitoring)
{{ config(
    materialized='table',
    tags=['monitoring']
) }}

WITH latest_runs AS (
  SELECT
    MAX(web_publication_date) AS latest_pub_date,
    COALESCE(MAX(dbt_updated_at), CURRENT_TIMESTAMP()) AS last_dbt_run,  -- COALESCE fallback if missing
    COUNTIF(web_publication_date >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))) AS recent_articles,  -- Last week
    CURRENT_TIMESTAMP() AS check_timestamp,
    -- Track row drops for quality gating: obt_articles keeps one row per valid article
    (SELECT COUNT(DISTINCT article_id) FROM {{ ref('stg_sf__guardian') }}) AS staging_total,
    COUNT(*) AS fact_total
  FROM {{ ref('obt_articles') }}
)

SELECT
  latest_pub_date,
  last_dbt_run,
  {{ to_local_ts('latest_pub_date') }} AS latest_pub_date_local,
  {{ to_local_ts('last_dbt_run') }} AS last_dbt_run_local,
  TIMESTAMP_DIFF(check_timestamp, latest_pub_date, HOUR) AS freshness_hours,
  recent_articles,
  CASE
    WHEN TIMESTAMP_DIFF(check_timestamp, latest_pub_date, HOUR) <= 24 THEN 'Fresh'
    WHEN TIMESTAMP_DIFF(check_timestamp, latest_pub_date, HOUR) <= 72 THEN 'Moderate'
    ELSE 'Stale - Alert!'
  END AS freshness_status,
  -- Data quality metrics
  staging_total,
  fact_total,
  staging_total - fact_total AS row_drop_count,
  ROUND((fact_total * 100.0 / NULLIF(staging_total, 0)), 2) AS retention_rate_percent,
  CASE
    WHEN (staging_total - fact_total) > 50 THEN 'High Drop - Review Filters'
    WHEN (staging_total - fact_total) > 0 THEN 'Minor Drop - Expected'
    ELSE 'Full Retention'
  END AS drop_status,
  check_timestamp,
  {{ to_local_ts('check_timestamp') }} AS check_timestamp_local
FROM latest_runs
ORDER BY check_timestamp DESC
LIMIT 1
