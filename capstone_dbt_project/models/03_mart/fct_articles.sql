{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='article_event_hk',
    cluster_by=['date_id', 'section_key', 'data_source']
) }}

WITH article_events AS (
  SELECT
    article_id,
    crawl_timestamp,
    web_publication_date,
    section_name,
    web_title,
    body_text,
    data_source,
    {{ generate_news_hash(['article_id', 'crawl_timestamp', 'section_name']) }} AS article_event_hk,
    MD5(CONCAT(
      COALESCE(article_id, ''),
      '|',
      COALESCE(web_title, ''),
      '|',
      COALESCE(section_name, ''),
      '|',
      MD5(COALESCE(body_text, ''))
    )) AS attribute_hash
  FROM {{ ref('stg_sf__guardian') }}
  WHERE
    web_publication_date >= '{{ var("start_date", "2001-01-01") }}'
    AND has_valid_title = TRUE
),

base_facts AS (
  SELECT
    ae.article_event_hk,
    ae.article_id,
    ae.crawl_timestamp,
    ae.web_publication_date,
    ae.attribute_hash,
    a.web_title,
    a.body_text,
    a.data_source,
    d.date_id,
    s.section_key,
    art.is_current,
    CASE WHEN a.has_valid_content THEN 1 ELSE 0 END AS valid_content,
    a.web_url,
    a.loaded_at
  FROM article_events ae
  JOIN {{ ref('stg_sf__guardian') }} a ON ae.article_id = a.article_id AND ae.crawl_timestamp = a.crawl_timestamp
  JOIN {{ ref('dim_date') }} d ON DATE(ae.web_publication_date) = d.date_id
  JOIN {{ ref('dim_sections') }} s ON UPPER(TRIM(ae.section_name)) = UPPER(TRIM(s.section_name))
  JOIN {{ ref('dim_articles') }} art ON ae.article_id = art.article_id AND art.is_current = TRUE
  {% if is_incremental() %}
    LEFT JOIN {{ this }} f ON ae.article_event_hk = f.article_event_hk
  {% endif %}
  WHERE
    ae.web_publication_date IS NOT NULL
    {% if is_incremental() %}
      AND (
        f.article_event_hk IS NULL
        {% set existing_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('upper') | list %}
        {% if 'ATTRIBUTE_HASH' in existing_columns %}
          OR ae.attribute_hash != COALESCE(f.attribute_hash, '00000000000000000000000000000000')
        {% endif %}
        OR ae.crawl_timestamp > (SELECT MAX(crawl_timestamp) FROM {{ this }})
        OR a.loaded_at > (SELECT MAX(dbt_updated_at) FROM {{ this }})
      )
    {% endif %}
)

SELECT
  article_event_hk,
  article_id,
  crawl_timestamp,
  web_publication_date,
  attribute_hash,
  web_title,
  body_text,
  data_source,
  date_id,
  section_key,
  is_current,
  valid_content,
  web_url,
  loaded_at,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM base_facts
