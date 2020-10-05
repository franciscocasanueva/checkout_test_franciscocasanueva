
        
        
    

    

    merge into `dbt-test-290911`.`dbt_francisco`.`postcode_log` as DBT_INTERNAL_DEST
        using (
           

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
          SELECT * FROM `dbt-test-290911`.`dbt_francisco`.`postcode_log` this WHERE this.postcode_id = to_hex(md5(cast(concat(coalesce(cast(users_extract.id as 
    string
), ''), '-', coalesce(cast(users_extract.postcode as 
    string
), '')) as 
    string
)))
        )


         ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.postcode_id = DBT_INTERNAL_DEST.postcode_id
        

    
    when matched then update set
        `postcode_id` = DBT_INTERNAL_SOURCE.`postcode_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`postcode` = DBT_INTERNAL_SOURCE.`postcode`,`postcode_updated_date` = DBT_INTERNAL_SOURCE.`postcode_updated_date`
    

    when not matched then insert
        (`postcode_id`, `user_id`, `postcode`, `postcode_updated_date`)
    values
        (`postcode_id`, `user_id`, `postcode`, `postcode_updated_date`)


  