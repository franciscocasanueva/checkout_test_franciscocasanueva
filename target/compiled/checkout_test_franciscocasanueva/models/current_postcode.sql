

select
  users_extract.id as user_id,
  users_extract.postcode,
  current_date() as postcode_updated_date
from
  users_extract
