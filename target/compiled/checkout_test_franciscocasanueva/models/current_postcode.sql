

select
  to_hex(md5(cast(concat(coalesce(cast(users_extract.id as 
    string
), ''), '-', coalesce(cast(users_extract.postcode as 
    string
), '')) as 
    string
))) as postcode_id,
  users_extract.id as user_id,
  users_extract.postcode,
  CURRENT_TIMESTAMP() as postcode_updated_date
from
  dbt_francisco.users_extract

  

    where
      NOT EXISTS (
          SELECT * FROM `dbt-test-290911`.`dbt_francisco`.`current_postcode` this WHERE this.postcode_id = to_hex(md5(cast(concat(coalesce(cast(users_extract.id as 
    string
), ''), '-', coalesce(cast(users_extract.postcode as 
    string
), '')) as 
    string
)))
        )

