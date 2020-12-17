CREATE TABLE exchange (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	code VARCHAR(10) NOT NULL, 
	name VARCHAR(50) NOT NULL,
	short_name VARCHAR(20) NOT NULL,
	country VARCHAR(50) NOT NULL,
	currency VARCHAR(50) NOT NULL,
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);