
        
        
    

    

    merge into `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg` as DBT_INTERNAL_DEST
        using (
           

with current_postcode as (
  select
    poscode_log1.user_id,
    poscode_log1.postcode,
    poscode_log1.postcode_updated_date
  from
    `dbt-test-290911`.`dbt_francisco`.`postcode_log` poscode_log1
  where
    poscode_log1.postcode_updated_date = (SELECT MAX(poscode_log2.postcode_updated_date) FROM `dbt-test-290911`.`dbt_francisco`.`postcode_log` poscode_log2 WHERE poscode_log1.user_id = poscode_log2.user_id)
)

select
  to_hex(md5(cast(concat(coalesce(cast(postcode_log.user_id as 
    string
), ''), '-', coalesce(cast(pageviews.pageview_datetime as 
    string
), '')) as 
    string
))) as update_postcode_id,
  postcode_log.user_id,
  pageviews.pageview_datetime,
  postcode_log.postcode as original_postcode,
  current_postcode.postcode as current_postcode,
  current_postcode.postcode_updated_date
from
  `dbt-test-290911`.`dbt_francisco`.`postcode_log` as postcode_log
  join dbt_francisco.pageviews_extract as pageviews on postcode_log.user_id = pageviews.user_id
  join current_postcode on current_postcode.user_id = postcode_log.user_id

where
  postcode_log.postcode_updated_date = (
      select
        max(postcode_log3.postcode_updated_date)
      from `dbt-test-290911`.`dbt_francisco`.`postcode_log` as postcode_log3
      where
        postcode_log3.user_id = postcode_log.user_id
        and postcode_log3.postcode_updated_date < pageviews.pageview_datetime)



      and current_postcode.postcode_updated_date > (select max(this.postcode_updated_date) from `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg` as this)


         ) as DBT_INTERNAL_SOURCE
        on 
            DBT_INTERNAL_SOURCE.update_postcode_id = DBT_INTERNAL_DEST.update_postcode_id
        

    
    when matched then update set
        `update_postcode_id` = DBT_INTERNAL_SOURCE.`update_postcode_id`,`user_id` = DBT_INTERNAL_SOURCE.`user_id`,`pageview_datetime` = DBT_INTERNAL_SOURCE.`pageview_datetime`,`original_postcode` = DBT_INTERNAL_SOURCE.`original_postcode`,`current_postcode` = DBT_INTERNAL_SOURCE.`current_postcode`,`postcode_updated_date` = DBT_INTERNAL_SOURCE.`postcode_updated_date`
    

    when not matched then insert
        (`update_postcode_id`, `user_id`, `pageview_datetime`, `original_postcode`, `current_postcode`, `postcode_updated_date`)
    values
        (`update_postcode_id`, `user_id`, `pageview_datetime`, `original_postcode`, `current_postcode`, `postcode_updated_date`)


  