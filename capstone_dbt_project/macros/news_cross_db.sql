-- macros/news_cross_db.sql (updated: use target.type for correct adapter detection)
{% macro news_date_spine(start_date, days) %}
    WITH RECURSIVE dates AS (
        SELECT {{ dbt.dateadd('day', 0, start_date) }} AS date_value
        UNION ALL
        SELECT {{ dbt.dateadd('day', 1, date_value) }} 
        FROM dates 
        WHERE {{ dbt.datediff('day', start_date, date_value) }} < {{ days }}
    )
    SELECT date_value FROM dates
{% endmacro %}

{% macro safe_numeric_cast(column) %}
    {% if target.type == 'snowflake' %}
        TRY_TO_NUMBER({{ column }})
    {% elif target.type == 'bigquery' %}
        SAFE_CAST({{ column }} AS NUMERIC)
    {% else %}
        CAST({{ column }} AS NUMERIC)
    {% endif %}
{% endmacro %}