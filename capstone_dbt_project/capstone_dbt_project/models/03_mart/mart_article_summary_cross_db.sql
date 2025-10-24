-- Config: Aggregated news summary with direct numerics (assumes pre-typed lengths; portable fallback for strings)
{{ config(materialized='table', cluster_by=['date_id', 'section_key']) }}

SELECT
    d.date_id,
    s.section_key,
    COUNT(DISTINCT f.article_id) AS article_count,
    SUM(f.body_length_chars) AS total_body_length_chars, 
    AVG(f.body_length_chars) AS avg_body_length_chars,
    SUM(f.valid_content_flag) AS valid_articles,
    ROUND((SUM(f.valid_content_flag) * 100.0 / NULLIF(COUNT(*), 0)), 2) AS valid_rate_percent,
    COUNT(DISTINCT f.web_url) AS unique_urls,
    AVG(f.title_to_body_ratio) AS avg_engagement_proxy,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('fct_advanced_articles') }} f
JOIN {{ ref('dim_date_cross_db') }} d ON f.date_id = d.date_id  -- Uses new portable dim
JOIN {{ ref('dim_sections') }} s ON f.section_key = s.section_key
GROUP BY 1, 2