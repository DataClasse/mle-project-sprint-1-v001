# dags/cleanin_flats
# _ETL.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from steps.clean_flats_data import create_table, extract, clean_data, load
from steps.messages import send_success, send_failure
from airflow.exceptions import AirflowException
import logging

# Вспомогательная функция для обработки ошибок и отправки уведомлений
def handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            error_msg = f"Возникла ошибка в задаче '{func.__name__}': {str(e)}"
            logging.error(error_msg)
            send_failure(kwargs['ti'], func.__name__, e)
            raise AirflowException(error_msg)
    return wrapper

# Оборачиваем наши функции обработчиком ошибок
wrapped_create_table = handle_errors(create_table)
wrapped_extract = handle_errors(extract)
wrapped_clean_data = handle_errors(clean_data)
wrapped_load = handle_errors(load)

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': lambda ctx: send_success(ctx, ctx['task_instance'].task_id),  
    'on_failure_callback': lambda ctx, err: send_failure(ctx, ctx['task_instance'].task_id, err), 
}

with DAG(
    dag_id='clean_real_estate_data',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=['ETL', 'clean_flats_data'],
    doc_md="""
### DAG для объединения данных о квартирах и зданиях

Объединяет данные из таблиц `flats` и `buildings` в единую таблицу `merged_flats_dataset`, проводит очистку данных и сохраняет результат в таблицу `clean_data_set`.
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

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=wrapped_clean_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=wrapped_load
    )

    # Очередность выполнения задач
    create_table_task >> extract_task >> clean_data_task >> load_task