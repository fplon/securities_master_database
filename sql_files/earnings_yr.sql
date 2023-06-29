CREATE TABLE earnings_yr (
	id BIGSERIAL NOT NULL PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
	interval_end_date DATE NOT NULL,
    eps_actual NUMERIC, 
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);