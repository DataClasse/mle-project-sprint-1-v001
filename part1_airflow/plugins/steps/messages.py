# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook
import os
import logging

logger = logging.getLogger(__name__)

MLE_TELEGRAM_TOKEN = os.getenv('MLE_TELEGRAM_TOKEN')
MLE_TELEGRAM_CHAT_ID = os.getenv('MLE_TELEGRAM_CHAT_ID')

def send_telegram_notification(context, success=True):
    """Базовая функция для отправки уведомлений"""
    try:
        hook = TelegramHook(token=MLE_TELEGRAM_TOKEN, chat_id=MLE_TELEGRAM_CHAT_ID)
        
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_instance = context.get('task_instance')
        
        if success:
            message = f"✅ DAG {dag_id} успешно выполнен!\nRun ID: {run_id}"
        else:
            exception = getattr(context, 'exception', None)
            error = str(exception) if exception else "Неизвестная ошибка"
            task_id = getattr(task_instance, 'task_id', 'N/A')
            message = (
                f"🔥 Ошибка в DAG {dag_id}!\n"
                f"Run ID: {run_id}\n"
                f"Задача: {task_id}\n"
                f"Ошибка: {error}"
            )
        
        hook.send_message(text=message)
        logger.info("Уведомление отправлено")
    
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {str(e)}")
        raise

def send_telegram_success_message(context):
    """Успешное выполнение DAG"""
    send_telegram_notification(context, success=True)

def send_telegram_failure_message(context):
    """Ошибка выполнения DAG"""
    send_telegram_notification(context, success=False)