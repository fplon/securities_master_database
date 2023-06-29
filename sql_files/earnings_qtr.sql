CREATE TABLE earnings_qtr (
	id BIGSERIAL NOT NULL PRIMARY KEY,
    instrument_id BIGINT NOT NULL,
    report_date DATE NOT NULL, 
    interval_end_date DATE NOT NULL, 
    before_after_market VARCHAR(50), 
    currency VARCHAR(50),
    eps_actual NUMERIC, 
    eps_estimate NUMERIC, 
    eps_difference NUMERIC, 
    surprise_percent NUMERIC,
	created_date DATE NOT NULL, 
	last_update_date DATE NOT NULL
);

-- CREATE TABLE earnings_qtr (
-- 	id BIGSERIAL NOT NULL PRIMARY KEY,
--     instrument_id BIGINT NOT NULL,


-- 	created_date DATE NOT NULL, 
-- 	last_update_date DATE NOT NULL
-- );