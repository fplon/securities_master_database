CREATE TABLE fund_watchlist (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	instrument_id BIGINT NOT NULL, 
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);