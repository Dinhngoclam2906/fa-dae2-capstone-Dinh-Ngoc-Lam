-- macros/news_business.sql (updated: remove unnecessary safe_cast for already-numeric body_length_chars)
{% macro news_engagement_score(body_len, title_ratio, valid_flag) %}
    -- RFM-like for news: Length * ratio * validity (body_len already numeric; no cast needed)
    GREATEST(0, ({{ body_len }} * {{ title_ratio }} * {{ valid_flag }})) / 1000.0
{% endmacro %}

{% macro dynamic_section_metrics(section_groups, metric_cols) %}
    SELECT
        section_group,
        {% for col in metric_cols %}
            {{ col }} AS {{ col | replace('SUM(', 'total_') }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM {{ ref('dim_sections') }}
    GROUP BY section_group
{% endmacro %}