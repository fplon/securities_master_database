CREATE TABLE instrument (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	exchange_id BIGINT NOT NULL,
	ticker VARCHAR(50) NOT NULL,
	instrument_type VARCHAR(50) NOT NULL, 
	name VARCHAR(200) NOT NULL,
	currency VARCHAR(50) NOT NULL,
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);