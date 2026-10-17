-- models/03_mart/dim_date.sql
{{ config(materialized='table', cluster_by=['year', 'month']) }}

WITH date_spine AS (
  -- BigQuery has a native date-array generator - no need for Snowflake's recursive CTE.
  SELECT date_id
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('{{ var("date_start", "2001-01-01") }}'),
      DATE_ADD(DATE('{{ var("date_start", "2001-01-01") }}'), INTERVAL {{ var("spine_days", 100000) - 1 }} DAY)
    )
  ) AS date_id
),

-- Simple hardcoded UK holidays (Guardian-focused; expand as seed if needed). A native
-- array-of-structs literal replaces Snowflake's PARSE_JSON + LATERAL FLATTEN.
holiday_dates AS (
  SELECT holiday_date, holiday_name
  FROM UNNEST([
    STRUCT(DATE('2010-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2010-12-27') AS holiday_date, 'Christmas (substitute)' AS holiday_name),
    STRUCT(DATE('2010-12-28') AS holiday_date, 'Boxing Day (substitute)' AS holiday_name),
    STRUCT(DATE('2011-01-03') AS holiday_date, "New Year's Day (substitute)" AS holiday_name),
    STRUCT(DATE('2011-04-22') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2011-04-25') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2011-05-02') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2011-08-29') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2011-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2011-12-27') AS holiday_date, 'Christmas Day (substitute)' AS holiday_name),
    STRUCT(DATE('2012-01-02') AS holiday_date, "New Year's Day (substitute)" AS holiday_name),
    STRUCT(DATE('2012-04-06') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2012-04-09') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2012-05-07') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2012-06-04') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2012-08-27') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2012-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2012-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2013-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2013-03-29') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2013-04-01') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2013-05-06') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2013-08-26') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2013-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2013-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2014-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2014-04-18') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2014-04-21') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2014-05-05') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2014-05-26') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2014-08-25') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2014-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2014-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2015-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2015-04-03') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2015-04-06') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2015-05-04') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2015-05-25') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2015-08-31') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2015-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2015-12-28') AS holiday_date, 'Boxing Day (substitute)' AS holiday_name),
    STRUCT(DATE('2016-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2016-03-25') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2016-03-28') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2016-05-02') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2016-05-30') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2016-08-29') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2016-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2016-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2017-01-02') AS holiday_date, "New Year's Day (substitute)" AS holiday_name),
    STRUCT(DATE('2017-04-14') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2017-04-17') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2017-05-01') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2017-05-29') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2017-08-28') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2017-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2017-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2018-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2018-01-02') AS holiday_date, '2nd January (Scotland)' AS holiday_name),
    STRUCT(DATE('2018-03-30') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2018-04-02') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2018-05-07') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2018-05-28') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2018-08-27') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2018-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2018-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2019-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2019-01-02') AS holiday_date, '2nd January (Scotland)' AS holiday_name),
    STRUCT(DATE('2019-04-19') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2019-04-22') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2019-05-06') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2019-05-27') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2019-08-26') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2019-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2019-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2020-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2020-01-02') AS holiday_date, '2nd January (Scotland)' AS holiday_name),
    STRUCT(DATE('2020-04-10') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2020-04-13') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2020-05-08') AS holiday_date, 'VE Day Bank Holiday' AS holiday_name),
    STRUCT(DATE('2020-05-25') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2020-08-31') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2020-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2020-12-28') AS holiday_date, 'Boxing Day (substitute)' AS holiday_name),
    STRUCT(DATE('2021-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2021-04-02') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2021-04-05') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2021-05-03') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2021-05-31') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2021-08-30') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2021-12-27') AS holiday_date, 'Christmas Day (substitute)' AS holiday_name),
    STRUCT(DATE('2021-12-28') AS holiday_date, 'Boxing Day (substitute)' AS holiday_name),
    STRUCT(DATE('2022-01-03') AS holiday_date, "New Year's Day (substitute)" AS holiday_name),
    STRUCT(DATE('2022-04-15') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2022-04-18') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2022-06-02') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2022-06-03') AS holiday_date, 'Platinum Jubilee Bank Holiday' AS holiday_name),
    STRUCT(DATE('2022-08-29') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2022-12-26') AS holiday_date, 'Christmas Day (substitute)' AS holiday_name),
    STRUCT(DATE('2022-12-27') AS holiday_date, 'Boxing Day (substitute)' AS holiday_name),
    STRUCT(DATE('2023-01-02') AS holiday_date, "New Year's Day (substitute)" AS holiday_name),
    STRUCT(DATE('2023-04-07') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2023-04-10') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2023-05-01') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2023-05-08') AS holiday_date, 'Coronation Bank Holiday' AS holiday_name),
    STRUCT(DATE('2023-08-28') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2023-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2023-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2024-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2024-03-29') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2024-04-01') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2024-05-06') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2024-05-27') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2024-08-26') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2024-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2024-12-26') AS holiday_date, 'Boxing Day' AS holiday_name),
    STRUCT(DATE('2025-01-01') AS holiday_date, "New Year's Day" AS holiday_name),
    STRUCT(DATE('2025-04-18') AS holiday_date, 'Good Friday' AS holiday_name),
    STRUCT(DATE('2025-04-21') AS holiday_date, 'Easter Monday' AS holiday_name),
    STRUCT(DATE('2025-05-05') AS holiday_date, 'Early May Bank Holiday' AS holiday_name),
    STRUCT(DATE('2025-05-26') AS holiday_date, 'Spring Bank Holiday' AS holiday_name),
    STRUCT(DATE('2025-08-25') AS holiday_date, 'Summer Bank Holiday' AS holiday_name),
    STRUCT(DATE('2025-12-25') AS holiday_date, 'Christmas Day' AS holiday_name),
    STRUCT(DATE('2025-12-26') AS holiday_date, 'Boxing Day' AS holiday_name)
  ])
),

date_attributes AS (
  SELECT
    ds.date_id,
    EXTRACT(YEAR FROM ds.date_id) AS year,
    EXTRACT(MONTH FROM ds.date_id) AS month,
    EXTRACT(DAY FROM ds.date_id) AS day,
    EXTRACT(DAYOFWEEK FROM ds.date_id) AS day_of_week,  -- BigQuery: Sunday=1 ... Saturday=7
    EXTRACT(QUARTER FROM ds.date_id) AS quarter,  -- noqa: RF04
    CASE
      WHEN EXTRACT(MONTH FROM ds.date_id) IN (1, 2, 3) THEN 'Q1'
      WHEN EXTRACT(MONTH FROM ds.date_id) IN (4, 5, 6) THEN 'Q2'
      WHEN EXTRACT(MONTH FROM ds.date_id) IN (7, 8, 9) THEN 'Q3'
      ELSE 'Q4'
    END AS fiscal_quarter,
    CASE WHEN EXTRACT(DAYOFWEEK FROM ds.date_id) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    COALESCE(h.holiday_date IS NOT NULL, FALSE) AS is_holiday,
    h.holiday_name
  FROM date_spine ds
  LEFT JOIN holiday_dates h ON ds.date_id = h.holiday_date
)

-- No trailing ORDER BY: BigQuery rejects "ORDER BY queries cannot be clustered" when
-- creating a table with cluster_by - clustering already organizes physical storage, so
-- an explicit row order here wouldn't be preserved anyway. Order at query time instead.
SELECT
  *,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
