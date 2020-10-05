{{
    config(
        materialized='incremental',
        unique_key='postcode_id',
        tag = ['daily']
    )
}}

select
  {{ dbt_utils.surrogate_key('users_extract.id', 'users_extract.postcode')}} as postcode_id,
  users_extract.id as user_id,
  users_extract.postcode,
  CURRENT_TIMESTAMP() as postcode_updated_date
from
  users_extract

  {% if is_incremental() %}

    where
      NOT EXISTS (
          SELECT * FROM {{ this }} this WHERE this.postcode_id = {{ dbt_utils.surrogate_key('users_extract.id', 'users_extract.postcode')}}
        )

{% endif %}
