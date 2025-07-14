DROP TABLE IF EXISTS STV202504021__DWH.global_metrics;

CREATE TABLE STV202504021__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from CHAR(3) NOT NULL,
    amount_total NUMERIC(18, 2) NOT NULL,  
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account NUMERIC(10, 2) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL,
    load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY DATE(date_update);