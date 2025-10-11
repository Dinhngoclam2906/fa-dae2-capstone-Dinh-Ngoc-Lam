
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select loaded_at
from DB_T26.STAGING.stg_sf__guardian
where loaded_at is null



  
  
      
    ) dbt_internal_test