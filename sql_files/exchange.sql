CREATE TABLE exchange (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	code VARCHAR(10) NOT NULL, 
	name VARCHAR(50) NOT NULL,
	short_name VARCHAR(20),
	country VARCHAR(50),
	currency VARCHAR(50),
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL,
	last_price_update_date DATE, 
	last_fundamental_update_date DATE
);