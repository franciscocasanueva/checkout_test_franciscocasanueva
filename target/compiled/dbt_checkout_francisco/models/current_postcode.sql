

select
  UE.id as user_id,
  UE.postcode,
  getdate() as postcode_updated_date
from
  users_extract as UE
