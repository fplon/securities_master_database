delete from daily_price 
where instrument_id = any(select id 
from instrument 
where exchange_id = 2);