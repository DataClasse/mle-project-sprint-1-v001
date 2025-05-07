from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from steps.messages import send_success, send_failure

def test_task():
    # Генерируем реальную ошибку
    raise Exception("Тестовая ошибка")

with DAG(
    dag_id="test_telegram_notifications",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    default_args={
        'on_success_callback': send_success,
        'on_failure_callback': send_failure,
    }
) as dag:

    PythonOperator(
        task_id="test_task",
        python_callable=test_task
    )