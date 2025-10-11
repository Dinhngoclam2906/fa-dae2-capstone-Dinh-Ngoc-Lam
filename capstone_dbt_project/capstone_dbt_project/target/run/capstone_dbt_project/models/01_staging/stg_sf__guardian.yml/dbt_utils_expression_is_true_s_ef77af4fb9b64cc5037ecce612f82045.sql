
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from DB_T26.STAGING_staging.stg_sf__guardian

where not(has_valid_title has_valid_title = TRUE)


  
  
      
    ) dbt_internal_test