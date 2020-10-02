{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'pageview_datetime'],
        tag = ['hourly']

    )
}}

--Add the new entries to the table
select
  pageviews.user_id,
  pageviews.pageview_datetime,
  USR.postcode as original_postcode,
  USR.postcode as current_postcode,
  USR.postcode_updated_date

from pageviews_extract pageviews
join {{ref("current_postcode")}} as USR on USR.user_id = pageviews.user_id

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where
    pageviews.pageview_datetime >= (select max(pageview_datetime) from {{ this }})

UNION

-- Update the old addresses that have changed
select
  updated.user_id,
  updated.pageview_datetime,
  updated.original_postcode,
  updated.current_postcode,
  updated.postcode_updated_date

from  {{ref("updated_postcode_stg")}} as updated
  -- this filter will only be applied on an incremental run
  where
    updated.postcode_updated_date >= (select max(postcode_updated_date) from {{ this }})

{% endif %}
