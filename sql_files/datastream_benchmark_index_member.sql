CREATE TABLE datastream_benchmark_index_member (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	index_id BIGINT NOT NULL,
	ds_code VARCHAR(255) NOT NULL, 
	name VARCHAR(255) NOT NULL,
	isin_code VARCHAR(255) NOT NULL,
	sedol_code VARCHAR(255) NOT NULL,
	index_date DATE NOT NULL, 
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);