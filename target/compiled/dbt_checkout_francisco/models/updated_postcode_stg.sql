



select
  current_postcode.user_id,
  pageviews.pageview_datetime,
  pageviews.original_postcode,
  current_postcode.postcode as current_postcode,
  current_postcode.postcode_updated_date
from
  `dbt-test-290911`.`dbt_francisco`.`current_postcode` as current_postcode
  left join {ref('pageviews_postcode')} pageviews on pageviews.user_id = post_curr.id
where
  current_postcode.postcode_updated_date > (select max(pageviews2.postcode_updated_date) from {ref('pageviews_postcode')} pageviews2)