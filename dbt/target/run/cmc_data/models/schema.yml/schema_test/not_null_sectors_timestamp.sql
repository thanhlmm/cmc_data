select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from "postgres"."public"."sectors"
where timestamp is null



      
    ) dbt_internal_test