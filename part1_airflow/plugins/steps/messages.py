# plugins/steps/messages.py
from telegram import Bot
import asyncio
import os
import logging
from airflow.models import Variable

MLE_TELEGRAM_TOKEN = Variable.get("MLE_TELEGRAM_TOKEN")
MLE_TELEGRAM_CHAT_ID = Variable.get("MLE_TELEGRAM_CHAT_ID")

#from dotenv import load_dotenv, find_dotenv

#load_dotenv(find_dotenv())
#MLE_TELEGRAM_TOKEN = os.getenv('MLE_TELEGRAM_TOKEN')
#MLE_TELEGRAM_CHAT_ID = os.getenv('MLE_TELEGRAM_CHAT_ID')

logger = logging.getLogger(__name__)

if not MLE_TELEGRAM_TOKEN:
    logger.error("MLE_TELEGRAM_TOKEN не найден!")
if not MLE_TELEGRAM_CHAT_ID:
    logger.error("MLE_TELEGRAM_CHAT_ID не найден!")

def _send_sync(message: str):
    """Синхронная обертка для асинхронной отправки"""
    try:
        bot = Bot(token=MLE_TELEGRAM_TOKEN)
        asyncio.run(bot.send_message(
            chat_id=MLE_TELEGRAM_CHAT_ID,
            text=message,
            parse_mode='HTML'
        ))
        logger.info(f"Сообщение отправлено: {message[:50]}...")
    except Exception as e:
        logger.error(f"Ошибка отправки: {str(e)}")
        raise

def send_notification(context, success: bool):
    """Универсальная функция для уведомлений"""
    dag_id = context['dag'].dag_id
    task_id = context.get('task_instance').task_id if not success else ''
    status = "✅ УСПЕХ" if success else "🔥 ОШИБКА"
    
    message = (
        f"<b>{status}</b>\n"
        f"DAG: {dag_id}\n"
        f"Run ID: {context['run_id']}"
    )
    
    if not success:
        error = str(context.get('exception')) or "Неизвестная ошибка"
        message += f"\nЗадача: {task_id}\nОшибка: <code>{error}</code>"

    _send_sync(message)

# Функции для Airflow callback
def send_success(context):
    send_notification(context, success=True)

def send_failure(context):
    send_notification(context, success=False)