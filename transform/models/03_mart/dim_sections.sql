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
  -- Every run re-merges all sections (a few hundred rows), so a change to
  -- section_group_macro reclassifies existing sections too. Incremental merge
  -- (not a full rebuild) keeps keys whose articles have left staging.
  WHERE section_name IS NOT NULL AND has_valid_section = TRUE
  GROUP BY 1
)

-- No trailing ORDER BY: BigQuery rejects it combined with cluster_by (see dim_date.sql).
SELECT
  section_key,
  section_name,
  {{ section_group_macro('section_name') }} AS section_group,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM normalized_sections
