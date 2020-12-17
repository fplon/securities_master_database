CREATE TABLE benchmark_index_member (
	index_id BIGINT NOT NULL,
	ticker VARCHAR(50) NOT NULL,
	exchange_code VARCHAR(50) NOT NULL,
	date DATE NOT NULL, 
	last_update_date DATE NOT NULL /*,
	PRIMARY KEY(ticker, exchange_code)*/
);