CREATE TABLE price (
	id BIGSERIAL NOT NULL PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
    price_date DATE NOT NULL, 
    open NUMERIC,
    high NUMERIC, 
    low NUMERIC, 
    close NUMERIC, 
    adjusted_close NUMERIC, 
    volume NUMERIC,
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);