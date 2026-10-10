{{ config(
    materialized='view',
    schema='marts'
) }}

SELECT
  a.article_id,
  LOWER(a.web_title) AS web_title_lower,
  LOWER(a.body_text) AS body_text_lower,
  a.web_title,
  a.web_publication_date,
  a.web_url,
  a.section_key,
  s.section_name,
  a.is_current,
  a.data_source,
  LEFT(a.body_text, 500) AS preview
FROM {{ ref('fct_articles') }} a
JOIN {{ ref('dim_sections') }} s ON a.section_key = s.section_key
WHERE a.is_current = TRUE
