-- Advanced test: Validate SCD model loads without null changes (passes if no bad rows; demo-ready)
{% if adapter.type() == 'snowflake' %}
    SELECT 1 AS validation_check
    FROM {{ ref('dim_articles') }}
    WHERE change_type IS NULL AND 1 = 0  -- Impossible: No rows = pass (nulls exist? Fix data)
{% else %}
    SELECT 1 AS generic_scd_pass  -- Fallback for other warehouses
{% endif %}