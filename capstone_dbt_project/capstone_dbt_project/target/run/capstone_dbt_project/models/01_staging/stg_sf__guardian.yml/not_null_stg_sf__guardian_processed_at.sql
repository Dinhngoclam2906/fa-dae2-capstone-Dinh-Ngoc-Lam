
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select processed_at
from DB_T26.STAGING_staging.stg_sf__guardian
where processed_at is null



  
  
      
    ) dbt_internal_test