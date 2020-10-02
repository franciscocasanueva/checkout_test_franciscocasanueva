
    
    



select count(*) as validation_errors
from (

    select
        user_id

    from `dbt-test-290911`.`dbt_francisco`.`current_postcode`
    where user_id is not null
    group by user_id
    having count(*) > 1

) validation_errors


