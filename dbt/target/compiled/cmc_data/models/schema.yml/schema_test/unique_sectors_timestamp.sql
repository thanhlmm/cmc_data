
    
    

select
    timestamp as unique_field,
    count(*) as n_records

from "postgres"."public"."sectors"
where timestamp is not null
group by timestamp
having count(*) > 1


