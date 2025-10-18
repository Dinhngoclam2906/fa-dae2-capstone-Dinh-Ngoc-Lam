{{ config(
    materialized='table',
    cluster_by=['year', 'month']
) }}

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2010-01-01'::DATE) as date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
),
date_attributes AS (
    SELECT
        date_value,
        -- Date components
        YEAR(date_value) as year,
        MONTH(date_value) as month,
        DAY(date_value) as day,
        DAYOFWEEK(date_value) as day_of_week,
        DAYOFYEAR(date_value) as day_of_year,
        -- Fiscal periods (adapt if your org uses different fiscal year)
        QUARTER(date_value) as quarter,
        CASE
            WHEN MONTH(date_value) IN (1, 2, 3) THEN 'Q1'
            WHEN MONTH(date_value) IN (4, 5, 6) THEN 'Q2'
            WHEN MONTH(date_value) IN (7, 8, 9) THEN 'Q3'
            ELSE 'Q4'
        END as fiscal_quarter,
        -- Business logic for news (e.g., weekends have lower publication volume?)
        CASE
            WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE
            ELSE FALSE
        END as is_weekend,
        CASE
            WHEN date_value = CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END as is_today,
        -- News-specific: Holidays? (Simple flag; expand with holiday table if needed)
        FALSE as is_holiday  -- Placeholder; integrate external holiday data if relevant
    FROM date_spine
    WHERE date_value <= CURRENT_DATE()
)
SELECT
    date_value as date_id,  -- Surrogate key for FKs
    *,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM date_attributes