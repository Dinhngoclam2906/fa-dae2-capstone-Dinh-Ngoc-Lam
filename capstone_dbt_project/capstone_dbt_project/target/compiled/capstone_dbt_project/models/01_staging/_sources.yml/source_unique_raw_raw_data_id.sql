
    
    

select
    id as unique_field,
    count(*) as n_records

from DB_T26.sc_t26.raw_data
where id is not null
group by id
having count(*) > 1


