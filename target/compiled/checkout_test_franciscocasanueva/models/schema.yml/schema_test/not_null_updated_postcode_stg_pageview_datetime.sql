
    
    



select count(*) as validation_errors
from `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg`
where pageview_datetime is null


