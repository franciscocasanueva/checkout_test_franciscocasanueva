
    
    



select count(*) as validation_errors
from `dbt-test-290911`.`dbt_francisco`.`current_postcode`
where postcode is null


