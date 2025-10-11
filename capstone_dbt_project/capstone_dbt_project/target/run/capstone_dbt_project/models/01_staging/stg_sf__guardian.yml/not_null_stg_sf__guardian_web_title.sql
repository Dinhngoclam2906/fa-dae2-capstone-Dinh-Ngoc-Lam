
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select web_title
from DB_T26.STAGING.stg_sf__guardian
where web_title is null



  
  
      
    ) dbt_internal_test