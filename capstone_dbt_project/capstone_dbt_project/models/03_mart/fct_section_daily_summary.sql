{{ config(
    materialized='table',
    cluster_by=['date_id', 'section_key']
) }}

SELECT
    d.date_id,
    s.section_key,
    -- Aggregated metrics
    COUNT(DISTINCT f.article_id) as article_count,
    SUM(f.body_length_chars) as total_body_length_chars,
    AVG(f.body_length_chars) as avg_body_length_chars,
    SUM(f.valid_content_flag) as valid_articles,
    ROUND((SUM(f.valid_content_flag) * 100.0 / COUNT(*)), 2) as valid_rate_percent,
    COUNT(DISTINCT f.web_url) as unique_urls,
    -- Business metrics
    AVG(f.title_to_body_ratio) as avg_engagement_proxy,
    -- Metadata
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM {{ ref('fct_articles') }} f
JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
JOIN {{ ref('dim_sections') }} s ON f.section_key = s.section_key
GROUP BY 1, 2