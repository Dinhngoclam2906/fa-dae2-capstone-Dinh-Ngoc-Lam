{{ config(materialized='ephemeral') }}

SELECT
  section_name,
  COUNT(*) AS article_count,
  ROUND(AVG(LENGTH(body_text)), 0) AS avg_body_length_chars,
  COUNT(DISTINCT web_url) AS unique_urls,
  MIN(crawl_timestamp) AS earliest_crawl,
  MAX(loaded_at) AS latest_load,
  SUM(CASE WHEN has_valid_content THEN 1 ELSE 0 END) AS valid_articles,
  CURRENT_TIMESTAMP() AS processed_at
FROM {{ ref('stg_sf__guardian') }}
GROUP BY section_name
HAVING article_count > 1
