{{ config(
    materialized='incremental',
    unique_key=['article_id', 'valid_from'],
    cluster_by=['article_id', 'valid_from']
) }}

WITH base_articles AS (
  SELECT
    article_id,
    web_title,
    web_url,
    section_name,
    crawl_timestamp,
    MD5(CONCAT(
      COALESCE(web_title, ''), '|', COALESCE(web_url, ''), '|',
      COALESCE(section_name, ''), '|', MD5(COALESCE(body_text, ''))
    )) AS content_change_hash,
    body_text
  FROM {{ ref('stg_sf__guardian') }}
  WHERE
    article_id IS NOT NULL
    AND has_valid_content = TRUE
  {% if is_incremental() %}
    AND crawl_timestamp > (SELECT MAX(valid_from) FROM {{ this }}) - INTERVAL '7 days'  -- Incremental: Recent crawls only
  {% endif %}
),

change_detection_raw AS (
  SELECT
    b.*,
    e.content_change_hash AS existing_content_change_hash,
    e.valid_from AS existing_valid_from,
    e.web_title AS existing_title,
    e.web_url AS existing_url
  FROM base_articles b
  {% if is_incremental() %}
    LEFT JOIN {{ this }} e ON b.article_id = e.article_id AND e.is_current = TRUE
  {% else %}
    CROSS JOIN (
      SELECT 
        NULL::STRING AS content_change_hash,
        NULL::TIMESTAMP_NTZ AS valid_from,
        NULL::STRING AS web_title,
        NULL::STRING AS web_url
    ) e
  {% endif %}
),

change_detection AS (
  SELECT
    *,
    CASE
      WHEN existing_content_change_hash IS NULL THEN 'new'
      WHEN content_change_hash != existing_content_change_hash THEN 'content_changed'
      ELSE 'unchanged'
    END AS change_type
  FROM change_detection_raw
  WHERE change_type != 'unchanged'  -- Filter early: Only new/changed
),

-- Generate closed copy of old version (for audit; appends alongside original)
closed_old_versions AS (
  SELECT
    cd.article_id,
    cd.existing_title AS web_title,
    cd.existing_url AS web_url,
    cd.section_name,
    {{ generate_news_hash(['cd.article_id', 'cd.existing_valid_from']) }} AS version_surrogate_key,
    cd.existing_valid_from AS valid_from,
    CURRENT_TIMESTAMP() AS valid_to,
    FALSE AS is_current,
    cd.existing_content_change_hash AS content_change_hash,
    'closed_by_change' AS change_type,
    cd.existing_valid_from AS crawl_timestamp,
    CURRENT_TIMESTAMP() AS dbt_updated_at
  FROM change_detection cd
  WHERE cd.change_type = 'content_changed'
),

-- Generate new/changed version
new_versions AS (
  SELECT
    cd.article_id,
    cd.web_title,
    cd.web_url,
    cd.section_name,
    {{ generate_news_hash(['cd.article_id', 'cd.crawl_timestamp', 'cd.content_change_hash']) }} AS version_surrogate_key,
    CASE
      WHEN cd.change_type = 'content_changed' THEN CURRENT_TIMESTAMP()
      ELSE COALESCE(cd.crawl_timestamp, CURRENT_TIMESTAMP())
    END AS valid_from,
    '9999-12-31'::TIMESTAMP_NTZ AS valid_to,
    TRUE AS is_current,
    cd.content_change_hash,
    cd.change_type,
    cd.crawl_timestamp,
    CURRENT_TIMESTAMP() AS dbt_updated_at
  FROM change_detection cd
  WHERE cd.change_type IN ('new', 'content_changed')
),

-- Union & join sections
joined_versions AS (
  SELECT
    v.*,
    s.section_key
  FROM (
    SELECT * FROM closed_old_versions
    UNION ALL
    SELECT * FROM new_versions
  ) v
  JOIN {{ ref('dim_sections') }} s ON UPPER(TRIM(v.section_name)) = s.section_key
)

SELECT
  article_id,
  web_title,
  web_url,
  section_key,
  valid_from,
  valid_to,
  is_current,
  version_surrogate_key,
  content_change_hash,
  change_type,
  crawl_timestamp,
  dbt_updated_at
FROM joined_versions
