

--Add the new entries to the table
select
  pageviews.user_id,
  pageviews.pageview_datetime,
  USR.postcode as original_postcode,
  USR.postcode as current_postcode,
  USR.postcode_updated_date

from pageviews_extract pageviews
join `dbt-test-290911`.`dbt_francisco`.`current_postcode` as USR on USR.user_id = pageviews.user_id

