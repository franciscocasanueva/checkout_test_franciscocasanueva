
    
    



select count(*) as validation_errors
from `dbt-test-290911`.`dbt_francisco`.`current_postcode`
where postcode_updated_date is null


