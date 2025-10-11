



select
    1
from (select * from DB_T26.STAGING_staging.stg_sf__guardian where has_valid_url = TRUE) dbt_subquery

where not(has_valid_url )

