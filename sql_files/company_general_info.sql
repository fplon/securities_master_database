CREATE TABLE company_general_info (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	instrument_id BIGINT NOT NULL,
	isin VARCHAR(50) ,
	gic_sector VARCHAR(50) ,
	gic_group VARCHAR(50) ,
	gic_industry VARCHAR(50) ,
	gic_subindustry VARCHAR(50) ,
	city VARCHAR(50) ,
	country VARCHAR(50) ,
	zip VARCHAR(50) , 
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);