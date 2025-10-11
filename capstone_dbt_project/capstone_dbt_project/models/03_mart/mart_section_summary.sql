{{ config(materialized='table', cluster_by=["section_name"]) }}

SELECT
    section_name,
    article_count,
    avg_body_length_chars,
    unique_urls,
    earliest_crawl,
    latest_load,
    valid_articles,
    ROUND((valid_articles * 100.0 / article_count), 2) AS valid_rate_percent,
    processed_at
FROM {{ ref('int_article_metrics') }}
ORDER BY article_count DESC