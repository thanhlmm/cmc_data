
    
    

select
    time as unique_field,
    count(*) as n_records

from "postgres"."public"."sectors"
where time is not null
group by time
having count(*) > 1


