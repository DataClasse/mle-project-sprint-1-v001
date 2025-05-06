# dags/real_estate_etl.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from steps.real_estate import create_table, extract, transform, load
from steps.messages import send_telegram_success_message, send_telegram_failure_message
from airflow.exceptions import AirflowException
import logging  # добавляем импорт модуля logging

# Вспомогательная функция для обработки ошибок и отправки уведомлений
def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            error_msg = f"Возникла ошибка в задаче '{func.__name__}': {str(e)}"
            logging.error(error_msg)  # теперь модуль logging доступен
            send_telegram_failure_message(error_msg)
            raise AirflowException(error_msg)
    return wrapper

# Оборачиваем наши функции обработчиком ошибок
wrapped_create_table = handle_errors(create_table)
wrapped_extract = handle_errors(extract)
wrapped_transform = handle_errors(transform)
wrapped_load = handle_errors(load)

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='real_estate_etl',
        schedule='@daily',
        start_date=datetime(2024, 1, 1),
        default_args=default_args,
        on_success_callback=send_telegram_success_message,
        on_failure_callback=send_telegram_failure_message,
        tags=['ETL', 'real_estate'],
        catchup=False,
        doc_md="""
### DAG для объединения данных о квартирах и зданиях

Этот DAG предназначен для регулярного объединения данных о недвижимости, включая квартиры и здания. Процесс включает четыре шага:
1. Создание целевой таблицы.
2. Извлечение сырых данных.
3. Трансформацию данных.
4. Загрузку трансформированных данных в базу данных.
""",
) as dag:

    create_table_task = PythonOperator(
        task_id='create_merged_table',
        python_callable=wrapped_create_table
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=wrapped_extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=wrapped_transform
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=wrapped_load
    )

    # Очередность выполнения задач
    create_table_task >> extract_task >> transform_task >> load_task