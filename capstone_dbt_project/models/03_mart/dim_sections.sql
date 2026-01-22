{{ config(
    materialized='incremental',
    unique_key='section_key',
    cluster_by=['section_key']
) }}

WITH normalized_sections AS (
  SELECT
    UPPER(TRIM(section_name)) AS section_key,
    MIN(section_name) AS section_name
  FROM {{ ref('stg_sf__guardian') }}
  WHERE
    section_name IS NOT NULL AND has_valid_section = TRUE
    {% if is_incremental() %}
      AND UPPER(TRIM(section_name)) NOT IN (SELECT section_key FROM {{ this }})
    {% endif %}
  GROUP BY 1
)

SELECT
  section_key,
  section_name,
  {{ section_group_macro('section_name') }} AS section_group,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM normalized_sections
ORDER BY section_name
