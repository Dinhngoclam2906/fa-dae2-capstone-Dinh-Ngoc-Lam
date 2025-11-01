-- models/03_mart/dim_date.sql
{{ config(materialized='table', cluster_by=['year', 'month']) }}

WITH date_spine AS (
  -- Embedded portable spine: Recursive CTE (Snowflake/BigQuery/Postgres-compatible)
  WITH RECURSIVE dates AS (
    SELECT DATE('{{ var("date_start", "2001-01-01") }}') AS date_id
    UNION ALL
    SELECT DATEADD('day', 1, date_id)
    FROM dates
    WHERE DATEDIFF('day', DATE('{{ var("date_start", "2001-01-01") }}'), date_id) < {{ var("spine_days", 100000) }}
  )

  SELECT date_id FROM dates
),

-- Simple hardcoded UK holidays (Guardian-focused; expand as seed if needed)
uk_holidays AS (
  SELECT PARSE_JSON('[
        {"date": "2010-01-01", "name": "New Year\'s Day"},
        {"date": "2010-12-27", "name": "Christmas (substitute)"},
        {"date": "2010-12-28", "name": "Boxing Day (substitute)"},
        {"date": "2011-01-03", "name": "New Year\'s Day (substitute)"},
        {"date": "2011-04-22", "name": "Good Friday"},
        {"date": "2011-04-25", "name": "Easter Monday"},
        {"date": "2011-05-02", "name": "Early May Bank Holiday"},
        {"date": "2011-08-29", "name": "Summer Bank Holiday"},
        {"date": "2011-12-26", "name": "Boxing Day"},
        {"date": "2011-12-27", "name": "Christmas Day (substitute)"},
        {"date": "2012-01-02", "name": "New Year\'s Day (substitute)"},
        {"date": "2012-04-06", "name": "Good Friday"},
        {"date": "2012-04-09", "name": "Easter Monday"},
        {"date": "2012-05-07", "name": "Early May Bank Holiday"},
        {"date": "2012-06-04", "name": "Spring Bank Holiday"},
        {"date": "2012-08-27", "name": "Summer Bank Holiday"},
        {"date": "2012-12-25", "name": "Christmas Day"},
        {"date": "2012-12-26", "name": "Boxing Day"},
        {"date": "2013-01-01", "name": "New Year\'s Day"},
        {"date": "2013-03-29", "name": "Good Friday"},
        {"date": "2013-04-01", "name": "Easter Monday"},
        {"date": "2013-05-06", "name": "Early May Bank Holiday"},
        {"date": "2013-08-26", "name": "Summer Bank Holiday"},
        {"date": "2013-12-25", "name": "Christmas Day"},
        {"date": "2013-12-26", "name": "Boxing Day"},
        {"date": "2014-01-01", "name": "New Year\'s Day"},
        {"date": "2014-04-18", "name": "Good Friday"},
        {"date": "2014-04-21", "name": "Easter Monday"},
        {"date": "2014-05-05", "name": "Early May Bank Holiday"},
        {"date": "2014-05-26", "name": "Spring Bank Holiday"},
        {"date": "2014-08-25", "name": "Summer Bank Holiday"},
        {"date": "2014-12-25", "name": "Christmas Day"},
        {"date": "2014-12-26", "name": "Boxing Day"},
        {"date": "2015-01-01", "name": "New Year\'s Day"},
        {"date": "2015-04-03", "name": "Good Friday"},
        {"date": "2015-04-06", "name": "Easter Monday"},
        {"date": "2015-05-04", "name": "Early May Bank Holiday"},
        {"date": "2015-05-25", "name": "Spring Bank Holiday"},
        {"date": "2015-08-31", "name": "Summer Bank Holiday"},
        {"date": "2015-12-25", "name": "Christmas Day"},
        {"date": "2015-12-28", "name": "Boxing Day (substitute)"},
        {"date": "2016-01-01", "name": "New Year\'s Day"},
        {"date": "2016-03-25", "name": "Good Friday"},
        {"date": "2016-03-28", "name": "Easter Monday"},
        {"date": "2016-05-02", "name": "Early May Bank Holiday"},
        {"date": "2016-05-30", "name": "Spring Bank Holiday"},
        {"date": "2016-08-29", "name": "Summer Bank Holiday"},
        {"date": "2016-12-25", "name": "Christmas Day"},
        {"date": "2016-12-26", "name": "Boxing Day"},
        {"date": "2017-01-02", "name": "New Year\'s Day (substitute)"},
        {"date": "2017-04-14", "name": "Good Friday"},
        {"date": "2017-04-17", "name": "Easter Monday"},
        {"date": "2017-05-01", "name": "Early May Bank Holiday"},
        {"date": "2017-05-29", "name": "Spring Bank Holiday"},
        {"date": "2017-08-28", "name": "Summer Bank Holiday"},
        {"date": "2017-12-25", "name": "Christmas Day"},
        {"date": "2017-12-26", "name": "Boxing Day"},
        {"date": "2018-01-01", "name": "New Year\'s Day"},
        {"date": "2018-01-02", "name": "2nd January (Scotland)"},
        {"date": "2018-03-30", "name": "Good Friday"},
        {"date": "2018-04-02", "name": "Easter Monday"},
        {"date": "2018-05-07", "name": "Early May Bank Holiday"},
        {"date": "2018-05-28", "name": "Spring Bank Holiday"},
        {"date": "2018-08-27", "name": "Summer Bank Holiday"},
        {"date": "2018-12-25", "name": "Christmas Day"},
        {"date": "2018-12-26", "name": "Boxing Day"},
        {"date": "2019-01-01", "name": "New Year\'s Day"},
        {"date": "2019-01-02", "name": "2nd January (Scotland)"},
        {"date": "2019-04-19", "name": "Good Friday"},
        {"date": "2019-04-22", "name": "Easter Monday"},
        {"date": "2019-05-06", "name": "Early May Bank Holiday"},
        {"date": "2019-05-27", "name": "Spring Bank Holiday"},
        {"date": "2019-08-26", "name": "Summer Bank Holiday"},
        {"date": "2019-12-25", "name": "Christmas Day"},
        {"date": "2019-12-26", "name": "Boxing Day"},
        {"date": "2020-01-01", "name": "New Year\'s Day"},
        {"date": "2020-01-02", "name": "2nd January (Scotland)"},
        {"date": "2020-04-10", "name": "Good Friday"},
        {"date": "2020-04-13", "name": "Easter Monday"},
        {"date": "2020-05-08", "name": "VE Day Bank Holiday"},
        {"date": "2020-05-25", "name": "Spring Bank Holiday"},
        {"date": "2020-08-31", "name": "Summer Bank Holiday"},
        {"date": "2020-12-25", "name": "Christmas Day"},
        {"date": "2020-12-28", "name": "Boxing Day (substitute)"},
        {"date": "2021-01-01", "name": "New Year\'s Day"},
        {"date": "2021-04-02", "name": "Good Friday"},
        {"date": "2021-04-05", "name": "Easter Monday"},
        {"date": "2021-05-03", "name": "Early May Bank Holiday"},
        {"date": "2021-05-31", "name": "Spring Bank Holiday"},
        {"date": "2021-08-30", "name": "Summer Bank Holiday"},
        {"date": "2021-12-27", "name": "Christmas Day (substitute)"},
        {"date": "2021-12-28", "name": "Boxing Day (substitute)"},
        {"date": "2022-01-03", "name": "New Year\'s Day (substitute)"},
        {"date": "2022-04-15", "name": "Good Friday"},
        {"date": "2022-04-18", "name": "Easter Monday"},
        {"date": "2022-06-02", "name": "Spring Bank Holiday"},
        {"date": "2022-06-03", "name": "Platinum Jubilee Bank Holiday"},
        {"date": "2022-08-29", "name": "Summer Bank Holiday"},
        {"date": "2022-12-26", "name": "Christmas Day (substitute)"},
        {"date": "2022-12-27", "name": "Boxing Day (substitute)"},
        {"date": "2023-01-02", "name": "New Year\'s Day (substitute)"},
        {"date": "2023-04-07", "name": "Good Friday"},
        {"date": "2023-04-10", "name": "Easter Monday"},
        {"date": "2023-05-01", "name": "Early May Bank Holiday"},
        {"date": "2023-05-08", "name": "Coronation Bank Holiday"},
        {"date": "2023-08-28", "name": "Summer Bank Holiday"},
        {"date": "2023-12-25", "name": "Christmas Day"},
        {"date": "2023-12-26", "name": "Boxing Day"},
        {"date": "2024-01-01", "name": "New Year\'s Day"},
        {"date": "2024-03-29", "name": "Good Friday"},
        {"date": "2024-04-01", "name": "Easter Monday"},
        {"date": "2024-05-06", "name": "Early May Bank Holiday"},
        {"date": "2024-05-27", "name": "Spring Bank Holiday"},
        {"date": "2024-08-26", "name": "Summer Bank Holiday"},
        {"date": "2024-12-25", "name": "Christmas Day"},
        {"date": "2024-12-26", "name": "Boxing Day"},
        {"date": "2025-01-01", "name": "New Year\'s Day"},
        {"date": "2025-04-18", "name": "Good Friday"},
        {"date": "2025-04-21", "name": "Easter Monday"},
        {"date": "2025-05-05", "name": "Early May Bank Holiday"},
        {"date": "2025-05-26", "name": "Spring Bank Holiday"},
        {"date": "2025-08-25", "name": "Summer Bank Holiday"},
        {"date": "2025-12-25", "name": "Christmas Day"},
        {"date": "2025-12-26", "name": "Boxing Day"}
    ]') AS holidays_array
),

holiday_dates AS (
  SELECT
    DATE(value:date::STRING) AS holiday_date,
    value:name::STRING AS holiday_name
  FROM uk_holidays,
    LATERAL FLATTEN(input => holidays_array)
),

date_attributes AS (
  SELECT
    ds.date_id,
    YEAR(ds.date_id) AS year,
    MONTH(ds.date_id) AS month,
    DAY(ds.date_id) AS day,
    DAYOFWEEK(ds.date_id) AS day_of_week,
    QUARTER(ds.date_id) AS quarter,
    CASE
      WHEN MONTH(ds.date_id) IN (1, 2, 3) THEN 'Q1'
      WHEN MONTH(ds.date_id) IN (4, 5, 6) THEN 'Q2'
      WHEN MONTH(ds.date_id) IN (7, 8, 9) THEN 'Q3'
      ELSE 'Q4'
    END AS fiscal_quarter,
    CASE WHEN DAYOFWEEK(ds.date_id) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    COALESCE(h.holiday_date IS NOT NULL, FALSE) AS is_holiday,
    h.holiday_name
  FROM date_spine ds
  LEFT JOIN holiday_dates h ON ds.date_id = h.holiday_date
)

SELECT
  *,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM date_attributes
ORDER BY date_id ASC
