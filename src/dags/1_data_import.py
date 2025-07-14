from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from vertica_python import connect
from confluent_kafka import Consumer, KafkaException
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_vertica_conn():
    """Получаем подключение к Vertica из Airflow Connection"""
    conn = BaseHook.get_connection('vertica_conn')
    return connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )


def get_kafka_config():
    """Получаем конфиг Kafka из Airflow Connection и Variables"""
    kafka_conn = BaseHook.get_connection('kafka_conn')

    return {
        'bootstrap.servers': kafka_conn.host,
        'group.id': Variable.get("kafka_group_id"),
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': kafka_conn.login,
        'sasl.password': kafka_conn.password
    }


def process_kafka_to_vertica(execution_date, **context):

    target_date = execution_date.date()

    kafka_conf = get_kafka_config()
    vertica_conn = get_vertica_conn()
    consumer = Consumer(kafka_conf)
    consumer.subscribe([Variable.get("kafka_topic")])

    try:
        vertica_cursor = vertica_conn.cursor()

        # Обработка сообщений
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode('utf-8'))

                message_date = datetime.strptime(
                    data['payload']['date'], '%Y-%m-%d %H:%M:%S').date()
                if message_date != target_date:
                    continue

                # Определяем тип сообщения
                if data['object_type'] == 'TRANSACTION':
                    # Вставка транзакции
                    vertica_cursor.execute("""
                        INSERT INTO STV202504021__STAGING.transactions (
                            operation_id, account_number_from, account_number_to,
                            currency_code, country, status, transaction_type,
                            amount, transaction_dt, load_dt, load_src
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        data['payload']['operation_id'],
                        str(data['payload']['account_number_from']),
                        str(data['payload']['account_number_to']),
                        str(data['payload']['currency_code']),
                        data['payload']['country'],
                        data['payload']['status'],
                        data['payload']['transaction_type'],
                        data['payload']['amount'],
                        data['payload']['transaction_dt'],
                        datetime.now(),
                        'kafka'
                    ))

                elif data['object_type'] == 'CURRENCY':
                    # Вставка курса валют
                    vertica_cursor.execute("""
                        INSERT INTO STV202504021__STAGING.currencies (
                            date_update, currency_code, currency_code_with,
                            currency_code_div, load_dt, load_src
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        data['payload']['date_update'],
                        data['payload']['currency_code'],
                        data['payload']['currency_code_with'],
                        data['payload']['currency_code_div'],
                        datetime.now(),
                        'kafka'
                    ))

                vertica_conn.commit()

            except Exception as e:
                print(f"Error processing message: {str(e)}")
                vertica_conn.rollback()

    finally:
        consumer.close()
        vertica_conn.close()


with DAG(
    'kafka_to_vertica_october_2022',
    default_args=default_args,
    description='Daily ETL from Kafka to Vertica for October 2022',
    schedule_interval='@daily',  # Запускаем ежедневно
    catchup=True  # Ддля обработки всех дней периода
) as dag:

    load_data = PythonOperator(
        task_id='load_daily_data',
        python_callable=process_kafka_to_vertica,
        provide_context=True
    )
