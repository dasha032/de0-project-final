DROP TABLE IF EXISTS STV202504021__STAGING.currencies;


CREATE TABLE STV202504021__STAGING.currencies (
    date_update TIMESTAMP NOT NULL,
    currency_code CHAR(3) NOT NULL,
    currency_code_with CHAR(3) NOT NULL,
    currency_code_div NUMERIC(10, 6) NOT NULL, 
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    load_src VARCHAR(20) NOT NULL DEFAULT 'kafka'
)
ORDER BY date_update, currency_code, currency_code_with
SEGMENTED BY HASH(date_update, currency_code) ALL NODES
PARTITION BY DATE(date_update::DATE);