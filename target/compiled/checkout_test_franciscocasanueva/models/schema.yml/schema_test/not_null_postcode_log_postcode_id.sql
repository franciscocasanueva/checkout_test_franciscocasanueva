
    
    



select count(*) as validation_errors
from `dbt-test-290911`.`dbt_francisco`.`postcode_log`
where postcode_id is null


