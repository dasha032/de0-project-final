DROP TABLE IF EXISTS STV202504021__STAGING.transactions;

CREATE TABLE STV202504021__STAGING.transactions (
    operation_id UUID NOT NULL,
    account_number_from VARCHAR(20) NOT NULL,
    account_number_to VARCHAR(20) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    country VARCHAR(50) NOT NULL,  
    status VARCHAR(20) NOT NULL, 
    transaction_type VARCHAR(30) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL,  
    transaction_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    load_src VARCHAR(20) NOT NULL DEFAULT 'kafka' 
)
ORDER BY operation_id, transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY DATE(transaction_dt::DATE);