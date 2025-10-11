
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from DB_T26.STAGING.stg_sf__guardian where has_valid_title = TRUE) dbt_subquery

where not(has_valid_title )


  
  
      
    ) dbt_internal_test