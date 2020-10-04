{{
    config(
      materialized='incremental',
      unique_key='update_postcode_id',
      tag = ['daily']
    )
}}

with current_postcode as (
  select
    poscode_log1.user_id,
    poscode_log1.postcode,
    poscode_log1.postcode_updated_date
  from
    {{ref ('postcode_log')}} poscode_log1
  where
    poscode_log1.postcode_updated_date = (SELECT MAX(poscode_log2.postcode_updated_date) FROM {{ref ('postcode_log')}} poscode_log2 WHERE poscode_log1.user_id = poscode_log2.user_id)
)

select
  {{ dbt_utils.surrogate_key('postcode_log.user_id', 'pageviews.pageview_datetime')}} as update_postcode_id,
  postcode_log.user_id,
  pageviews.pageview_datetime,
  postcode_log.postcode as original_postcode,
  current_postcode.postcode as current_postcode,
  current_postcode.postcode_updated_date
from
  {{ref ('postcode_log')}} as postcode_log
  join dbt_francisco.pageviews_extract as pageviews on postcode_log.user_id = pageviews.user_id
  join current_postcode on current_postcode.user_id = postcode_log.user_id

where
  postcode_log.postcode_updated_date = (
      select
        max(postcode_log3.postcode_updated_date)
      from {{ref ('postcode_log')}} as postcode_log3
      where
        postcode_log3.user_id = postcode_log.user_id
        and postcode_log3.postcode_updated_date < pageviews.pageview_datetime)

{% if is_incremental() %}

      and current_postcode.postcode_updated_date > (select max(this.postcode_updated_date) from {{this}} as this)

{% endif %}
