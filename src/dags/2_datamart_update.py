from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import vertica_python

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_vertica_conn():
    """Получаем подключение к Vertica из Airflow Connection"""
    conn = BaseHook.get_connection('vertica_conn')
    return vertica_python.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
def update_global_metrics():
    conn_info = get_vertica_conn()

    with vertica_python.connect(**conn_info) as conn:
        curs = conn.cursor()

        # SQL для инкрементального обновления
        sql = """
        INSERT INTO STV202504021__DWH.global_metrics
        WITH yesterday_transactions AS (
            SELECT
                DATE(t.transaction_dt) AS date_update,
                t.currency_code AS currency_from,
                t.account_number_from,
                COUNT(*) AS cnt_user_transactions,
                SUM(t.amount) AS amount_original_currency
            FROM STV202504021__STAGING.transactions t
            WHERE t.status = 'done'
            AND account_number_from !~ '^[0\\-]'
            AND DATE(t.transaction_dt) = CURRENT_DATE - 1
            GROUP BY DATE(t.transaction_dt), t.currency_code, t.account_number_from
        ),
        currency_rates AS (
            SELECT
                DATE(c.date_update) AS date_update,
                c.currency_code,
                c.currency_code_div
            FROM STV202504021__STAGING.currencies c
            WHERE DATE(c.date_update) = CURRENT_DATE - 1
        )
        SELECT
            yt.date_update,
            yt.currency_from,
            SUM(yt.amount_original_currency * cr.currency_code_div) AS amount_total,
            SUM(yt.cnt_user_transactions) AS cnt_transactions,
            ROUND(AVG(yt.cnt_user_transactions), 2) AS avg_transactions_per_account,
            COUNT(DISTINCT yt.account_number_from) AS cnt_accounts_make_transactions,
            CURRENT_TIMESTAMP AS load_dt
        FROM yesterday_transactions yt
        LEFT JOIN currency_rates cr ON 
            yt.currency_from = cr.currency_code
        GROUP BY yt.date_update, yt.currency_from;
        """
        curs.execute(sql)
        curs.commit()


with DAG(
    'update_global_metrics',
    default_args=default_args,
    description='Daily incremental update of global_metrics table',
    schedule_interval='0 2 * * *',  # Ежедневно в 02:00
    catchup=False
) as dag:

    update_task = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_global_metrics
    )
