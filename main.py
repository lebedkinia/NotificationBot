import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from config import BOT_TOKEN
from database import ReminderDatabase
from kafka_handler import KafkaNotificationHandler, start_kafka_consumer
from kafka.errors import NoBrokersAvailable
import logging
from datetime import datetime, timedelta
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Initialize Kafka handler
try:
    kafka_handler = KafkaNotificationHandler()
    logger.info("Kafka handler initialized successfully")
except NoBrokersAvailable as e:
    logger.error(f"Failed to initialize Kafka handler: {e}")
    raise

class ReminderStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_start_time = State()
    waiting_for_end_time = State()
    waiting_for_frequency = State()

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Привет! Я бот для напоминаний. Напиши /help для команд.")

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "Доступные команды:\n"
        "/create_remind - создать новое напоминание\n"
        "/list_reminds - показать список напоминаний\n"
        "/delete_remind - удалить напоминание"
    )

@dp.message(Command("create_remind"))
async def create_remind(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, введите текст напоминания:")
    await state.set_state(ReminderStates.waiting_for_text)

@dp.message(ReminderStates.waiting_for_text)
async def process_text(message: Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer(
        "Теперь введите время начала напоминаний в формате ДД.ММ.ГГГГ ЧЧ:ММ\n"
        "Например: 01.01.2024 12:00"
    )
    await state.set_state(ReminderStates.waiting_for_start_time)

@dp.message(ReminderStates.waiting_for_start_time)
async def process_start_time(message: Message, state: FSMContext):
    try:
        start_time = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        await state.update_data(start_time=start_time)
        await message.answer(
            "Теперь введите время окончания напоминаний в том же формате:\n"
            "ДД.ММ.ГГГГ ЧЧ:ММ"
        )
        await state.set_state(ReminderStates.waiting_for_end_time)
    except ValueError:
        await message.answer(
            "Неверный формат даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ ЧЧ:ММ\n"
            "Например: 01.01.2024 12:00"
        )

@dp.message(ReminderStates.waiting_for_end_time)
async def process_end_time(message: Message, state: FSMContext):
    try:
        end_time = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        data = await state.get_data()
        start_time = data['start_time']
        
        if end_time <= start_time:
            await message.answer("Время окончания должно быть позже времени начала. Попробуйте снова:")
            return
            
        await state.update_data(end_time=end_time)
        await message.answer("Введите частоту напоминаний в минутах (целое число):")
        await state.set_state(ReminderStates.waiting_for_frequency)
    except ValueError:
        await message.answer(
            "Неверный формат даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ ЧЧ:ММ\n"
            "Например: 01.01.2024 12:00"
        )

@dp.message(ReminderStates.waiting_for_frequency)
async def process_frequency(message: Message, state: FSMContext):
    try:
        frequency = int(message.text)
        if frequency <= 0:
            await message.answer("Частота должна быть положительным числом. Попробуйте снова:")
            return
            
        data = await state.get_data()
        start_time = data['start_time']
        end_time = data['end_time']
        text = data['text']
        
        # Generate reminders and send to Kafka
        current_time = start_time
        remind_id = 1  # You might want to generate unique IDs
        
        while current_time <= end_time:
            notification_data = {
                'chat_id': message.from_user.id,
                'remind_id': remind_id,
                'text': text,
                'time': current_time.strftime("%Y-%m-%d %H:%M:%S+00")
            }
            kafka_handler.send_notification(notification_data)
            remind_id += 1
            current_time += timedelta(minutes=frequency)
        
        await message.answer(
            f"Отлично! Напоминания созданы:\n"
            f"Текст: {text}\n"
            f"С {start_time.strftime('%d.%m.%Y %H:%M')} до {end_time.strftime('%d.%m.%Y %H:%M')}\n"
            f"Каждые {frequency} минут"
        )
        await state.clear()
        
    except ValueError:
        await message.answer("Пожалуйста, введите целое число для частоты напоминаний:")

async def main():
    try:
        # Start Kafka consumer in a separate task
        asyncio.create_task(asyncio.to_thread(start_kafka_consumer))
        logger.info("Kafka consumer started")
        
        # Start the bot
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
