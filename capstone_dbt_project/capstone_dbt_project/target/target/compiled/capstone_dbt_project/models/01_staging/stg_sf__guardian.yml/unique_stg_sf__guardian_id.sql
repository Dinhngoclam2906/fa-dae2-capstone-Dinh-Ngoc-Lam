
    
    

select
    id as unique_field,
    count(*) as n_records

from DB_T26.STAGING_staging.stg_sf__guardian
where id is not null
group by id
having count(*) > 1


