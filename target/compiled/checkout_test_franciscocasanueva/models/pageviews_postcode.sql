

--depends_on: `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg`

--Add the new entries to the table
select
  to_hex(md5(cast(concat(coalesce(cast(pageviews.user_id as 
    string
), ''), '-', coalesce(cast(pageviews.pageview_datetime as 
    string
), '')) as 
    string
))) as uniquekey,
  pageviews.user_id,
  pageviews.pageview_datetime,
  USR.postcode as original_postcode,
  USR.postcode as current_postcode,
  USR.postcode_updated_date

from dbt_francisco.pageviews_extract as pageviews
join `dbt-test-290911`.`dbt_francisco`.`postcode_log` as USR on USR.user_id = pageviews.user_id


  -- this filter will only be applied on an incremental run
  where
    pageviews.pageview_datetime > (select max(this.pageview_datetime) from `dbt-test-290911`.`dbt_francisco`.`pageviews_postcode` as this)


  --Update those postcodes that have changed
  UNION ALL

  select
    to_hex(md5(cast(concat(coalesce(cast(updated.user_id as 
    string
), ''), '-', coalesce(cast(updated.pageview_datetime as 
    string
), '')) as 
    string
))) as uniquekey,
    updated.user_id,
    updated.pageview_datetime,
    updated.original_postcode,
    updated.current_postcode,
    updated.postcode_updated_date

  from  `dbt-test-290911`.`dbt_francisco`.`updated_postcode_stg` as updated

    where
      updated.postcode_updated_date > (select max(this.postcode_updated_date) from `dbt-test-290911`.`dbt_francisco`.`pageviews_postcode` as this)
