import os

# Paths
models_dir = 'models/03_mart'
dim_files = ['dim_publication_dates.sql', 'dim_crawl_dates.sql', 'dim_load_dates.sql', 'dim_sections.sql']  # Include load for now; delete after

# Clean code for each file (no LOADED_AT, fixed SCD)
dim_pub_code = '''{{ config(materialized='table') }}

WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))
),
date_spine AS (
  SELECT DATEADD(DAY, n, '2010-01-01'::DATE) AS full_date
  FROM numbers
),
date_attributes AS (
  SELECT
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    DAYOFYEAR(full_date) AS day_of_year,
    QUARTER(full_date) AS quarter,
    CASE
      WHEN MONTH(full_date) IN (1, 2, 3) THEN 'Q1'
      WHEN MONTH(full_date) IN (4, 5, 6) THEN 'Q2'
      WHEN MONTH(full_date) IN (7, 8, 9) THEN 'Q3'
      ELSE 'Q4'
    END AS fiscal_quarter,
    CASE WHEN DAYOFWEEK(full_date) IN (2, 3, 4, 5, 6) THEN TRUE ELSE FALSE END AS is_weekday,
    CASE WHEN full_date = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today
  FROM date_spine
  WHERE full_date <= DATEADD(YEAR, 1, CURRENT_DATE())
)
SELECT
  ROW_NUMBER() OVER (ORDER BY full_date) AS pub_date_key,
  *,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
ORDER BY full_date;'''

dim_crawl_code = '''{{ config(materialized='table') }}

WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))
),
date_spine AS (
  SELECT DATEADD(DAY, n, '2010-01-01'::DATE) AS full_date
  FROM numbers
),
date_attributes AS (
  SELECT
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    QUARTER(full_date) AS quarter,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN full_date = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today
  FROM date_spine
  WHERE full_date <= DATEADD(YEAR, 1, CURRENT_DATE())
)
SELECT
  ROW_NUMBER() OVER (ORDER BY full_date) AS crawl_date_key,
  *,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
ORDER BY full_date;'''

dim_load_code = '''{{ config(materialized='table') }}

WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))
),
date_spine AS (
  SELECT DATEADD(DAY, n, '2010-01-01'::DATE) AS full_date
  FROM numbers
),
date_attributes AS (
  SELECT
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    QUARTER(full_date) AS quarter,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN full_date = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today
  FROM date_spine
  WHERE full_date <= DATEADD(YEAR, 1, CURRENT_DATE())
)
SELECT
  ROW_NUMBER() OVER (ORDER BY full_date) AS load_date_key,
  *,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
ORDER BY full_date;'''

dim_sections_code = '''{{ config(
  materialized='incremental',
  unique_key='section_name',
  cluster_by=['section_name']
) }}

WITH section_changes AS (
  SELECT
    section_name,
    section_name AS section_description,
    MIN(crawl_timestamp) AS created_at,
    MD5(section_name) AS change_hash
  FROM {{ ref('stg_sf__guardian') }}
  WHERE section_name IS NOT NULL
  GROUP BY section_name
),
existing_sections AS (
  {% if is_incremental() %}
  SELECT
    section_name,
    change_hash,
    valid_from,
    valid_to,
    is_current
  FROM {{ this }}
  WHERE is_current = TRUE
  {% else %}
  SELECT NULL::VARCHAR AS section_name, NULL::VARCHAR AS change_hash, NULL::TIMESTAMP AS valid_from, NULL::TIMESTAMP AS valid_to, NULL::BOOLEAN AS is_current
  {% endif %}
),
change_detection AS (
  SELECT
    s.*,
    e.change_hash AS existing_hash,
    e.valid_from,
    e.valid_to,
    e.is_current,
    CASE
      WHEN e.existing_hash IS NULL THEN 'new'
      WHEN s.change_hash != e.existing_hash THEN 'changed'
      ELSE 'unchanged'
    END AS change_type
  FROM section_changes s
  LEFT JOIN existing_sections e ON s.section_name = e.section_name
),
all_records AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY cd.section_name, cd.created_at) AS section_key,
    cd.section_name,
    cd.section_description,
    cd.created_at,
    cd.change_hash,
    CASE
      WHEN cd.change_type IN ('new', 'changed') THEN cd.created_at
      ELSE COALESCE(cd.valid_from, cd.created_at)
    END AS valid_from,
    CASE
      WHEN cd.change_type = 'changed' THEN CURRENT_TIMESTAMP()
      ELSE COALESCE(cd.valid_to, '9999-12-31'::TIMESTAMP)
    END AS valid_to,
    CASE
      WHEN cd.change_type = 'changed' THEN FALSE
      ELSE COALESCE(cd.is_current, TRUE)
    END AS is_current,
    cd.change_type
  FROM change_detection cd
  WHERE cd.change_type IN ('new', 'changed')
    OR cd.change_type = 'unchanged'
  {% if is_incremental() %}
  AND cd.change_type != 'unchanged'
  {% endif %}
)
SELECT
  section_key,
  section_name,
  section_description,
  created_at,
  change_hash,
  valid_from,
  valid_to,
  is_current,
  change_type,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM all_records
ORDER BY section_name, valid_from;'''

# Write files
for f in dim_files:
    if f == 'dim_publication_dates.sql':
        with open(os.path.join(models_dir, f), 'w') as file:
            file.write(dim_pub_code)
    elif f == 'dim_crawl_dates.sql':
        with open(os.path.join(models_dir, f), 'w') as file:
            file.write(dim_crawl_code)
    elif f == 'dim_load_dates.sql':
        with open(os.path.join(models_dir, f), 'w') as file:
            file.write(dim_load_code)
    elif f == 'dim_sections.sql':
        with open(os.path.join(models_dir, f), 'w') as file:
            file.write(dim_sections_code)

# Verify no bad strings
bad_strings = ['LOADED_AT', 'loaded_at', 'SECTION_KEY']
for f in dim_files:
    with open(os.path.join(models_dir, f), 'r') as file:
        content = file.read()
        for bad in bad_strings:
            if bad in content:
                print(f"ERROR: '{bad}' in {f}")
                exit(1)
print("Files written and verified clean!")

# Optional: Run dbt clean
os.system('dbt clean')
print("dbt clean run. Now run 'dbt compile --select tag:dimensions' then 'dbt run --select tag:dimensions'")