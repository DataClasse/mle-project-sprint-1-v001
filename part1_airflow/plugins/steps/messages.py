# plugins/steps/messages.py
from telegram import Bot
import asyncio
import logging
from airflow.models import Variable

MLE_TELEGRAM_TOKEN = Variable.get("MLE_TELEGRAM_TOKEN")
MLE_TELEGRAM_CHAT_ID = Variable.get("MLE_TELEGRAM_CHAT_ID")

logger = logging.getLogger(__name__)

def _send(message: str):
    try:
        bot = Bot(token=MLE_TELEGRAM_TOKEN)
        asyncio.run(bot.send_message(
            chat_id=MLE_TELEGRAM_CHAT_ID,
            text=message,
            parse_mode='HTML'
        ))
    except Exception as e:
        logging.error(f"Ошибка отправки: {str(e)}")

def send_success(context, task_name: str):
    """Отправляет сообщение о успешном завершении задачи."""
    message = (
        f"✅ <b>Задача {task_name} успешно завершена!</b>\n"
        f"DAG: {context['dag'].dag_id}\n"
        f"Run ID: {context['run_id']}\n"
    )
    _send(message)

def send_failure(context, task_name: str, error: Exception):
    """Отправляет сообщение о неудаче выполнения задачи."""
    message = (
        f"🔥 <b>Ошибка в задаче {task_name}!</b>\n"
        f"DAG: {context['dag'].dag_id}\n"
        f"Run ID: {context['run_id']}\n"
        f"Ошибка: <code>{str(error)}</code>"
    )
    _send(message)