
    
    



select count(*) as validation_errors
from (

    select
        update_postcode_id

    from `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg`
    where update_postcode_id is not null
    group by update_postcode_id
    having count(*) > 1

) validation_errors


