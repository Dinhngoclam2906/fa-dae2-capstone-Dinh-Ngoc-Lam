{{ config(
    materialized='view',
    schema='marts'
) }}

-- The agent's SQL lexical search. Reads the obt_articles OBT (one row per article);
-- section_key / is_current are kept so the existing agent queries still work.
SELECT
  article_id,
  LOWER(web_title) AS web_title_lower,
  LOWER(body_text) AS body_text_lower,
  web_title,
  web_publication_date,
  web_url,
  UPPER(section_name) AS section_key,
  section_name,
  section_group,
  TRUE AS is_current,
  data_source,
  source_name,
  language,  -- noqa: RF04
  preview
FROM {{ ref('obt_articles') }}
