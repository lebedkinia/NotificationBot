from os import getenv
from dotenv import load_dotenv

load_dotenv(r"notbot.env")
# Токен Telegram-бота
BOT_TOKEN = getenv("TOKEN")
ADMIN_CHAT_ID = getenv("ADMIN_CHAT_ID")

