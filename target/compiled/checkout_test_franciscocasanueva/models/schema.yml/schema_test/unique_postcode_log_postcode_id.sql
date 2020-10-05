
    
    



select count(*) as validation_errors
from (

    select
        postcode_id

    from `dbt-test-290911`.`dbt_francisco`.`postcode_log`
    where postcode_id is not null
    group by postcode_id
    having count(*) > 1

) validation_errors


