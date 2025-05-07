from telegram import Bot
import asyncio
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv()

MLE_TELEGRAM_TOKEN = os.getenv('MLE_TELEGRAM_TOKEN')
MLE_TELEGRAM_CHAT_ID = os.getenv('MLE_TELEGRAM_CHAT_ID')


async def send_message(bot_token, chat_id, message_text):
    async with Bot(token=bot_token) as bot:
        await bot.send_message(chat_id=chat_id, text=message_text)

# Замените значения ниже своими данными
# BOT_TOKEN = MLE_TELEGRAM_TOKEN
# CHAT_ID = MLE_TELEGRAM_CHAT_ID  # Или номер чата, например '-123456789'
MESSAGE_TEXT = 'Привет! Это тестовое сообщение.'

asyncio.run(send_message(MLE_TELEGRAM_TOKEN, MLE_TELEGRAM_CHAT_ID, MESSAGE_TEXT))