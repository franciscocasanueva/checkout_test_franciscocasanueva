{{
    config(
        materialized='incremental',
        unique_key='update_postcode_id',
        tag = ['hourly']

    )
}}

--depends_on: {{ ref('updated_postcode_stg') }}

--Add the new entries to the table
select
  {{ dbt_utils.surrogate_key('pageviews.user_id', 'pageviews.pageview_datetime') }} as update_postcode_id,
  pageviews.user_id,
  pageviews.pageview_datetime,
  USR.postcode as original_postcode,
  USR.postcode as current_postcode,
  USR.postcode_updated_date

from pageviews_extract as pageviews
join {{ref('postcode_log')}} as USR on USR.user_id = pageviews.user_id

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where
    pageviews.pageview_datetime > (select max(this.pageview_datetime) from {{ this }} as this)


  --Update those postcodes that have changed
  UNION ALL

  select
    {{ dbt_utils.surrogate_key('updated.user_id', 'updated.pageview_datetime') }} as update_postcode_id,
    updated.user_id,
    updated.pageview_datetime,
    updated.original_postcode,
    updated.current_postcode,
    updated.postcode_updated_date

  from  {{ref('updated_postcode_stg')}} as updated

    where
      updated.postcode_updated_date > (select max(this.postcode_updated_date) from {{ this }} as this)
{% endif %}
