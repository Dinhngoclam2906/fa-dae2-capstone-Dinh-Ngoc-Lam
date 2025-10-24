-- Monitoring model for news freshness (run with: dbt run --select tag:monitoring)
{{ config(
    materialized='table', 
    tags=['monitoring']
) }}

WITH latest_runs AS (
    SELECT
        MAX(web_publication_date) AS latest_pub_date,
        COALESCE(MAX(DBT_UPDATED_AT), CURRENT_TIMESTAMP()) AS last_dbt_run,  -- Uppercase + COALESCE fallback if missing
        COUNT(DISTINCT article_id) AS recent_articles,
        CURRENT_TIMESTAMP() AS check_timestamp
    FROM {{ ref('fct_advanced_articles') }} f
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
    check_timestamp
FROM latest_runs
ORDER BY check_timestamp DESC
LIMIT 1