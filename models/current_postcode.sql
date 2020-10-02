{{
    config(
        materialized='incremental',
        unique_key=['id'],
        tag = ['daily']
    )
}}

select
  users_extract.id as user_id,
  users_extract.postcode,
  current_date() as postcode_updated_date
from
  users_extract
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where
      NOT EXISTS (
          SELECT * FROM {{ this }} this WHERE this.id = users_extract.id AND this.postcode = users_extract.postcode
        )

{% endif %}
