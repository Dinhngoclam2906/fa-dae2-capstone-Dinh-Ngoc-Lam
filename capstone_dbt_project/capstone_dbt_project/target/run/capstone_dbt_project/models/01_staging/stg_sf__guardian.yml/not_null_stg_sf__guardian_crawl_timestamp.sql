
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select crawl_timestamp
from DB_T26.STAGING.stg_sf__guardian
where crawl_timestamp is null



  
  
      
    ) dbt_internal_test