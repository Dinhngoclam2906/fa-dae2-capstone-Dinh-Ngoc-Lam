
    
    

select
    article_id as unique_field,
    count(*) as n_records

from DB_T26.sc_t26.raw_data
where article_id is not null
group by article_id
having count(*) > 1


