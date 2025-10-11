
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from DB_T26.STAGING_staging.stg_sf__guardian
where id is null



  
  
      
    ) dbt_internal_test