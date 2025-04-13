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
from reminder_sender import start_reminder_sender
import logging
from datetime import datetime, timedelta, timezone
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set your local timezone (GMT+3)
LOCAL_TIMEZONE = timezone(timedelta(hours=3))

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
        # Parse local time and convert to UTC
        local_time = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        local_time = local_time.replace(tzinfo=LOCAL_TIMEZONE)
        start_time = local_time.astimezone(timezone.utc)
        
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
        # Parse local time and convert to UTC
        local_time = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        local_time = local_time.replace(tzinfo=LOCAL_TIMEZONE)
        end_time = local_time.astimezone(timezone.utc)
        
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
        
        # Convert times back to local timezone for display
        local_start = start_time.astimezone(LOCAL_TIMEZONE)
        local_end = end_time.astimezone(LOCAL_TIMEZONE)
        
        await message.answer(
            f"Отлично! Напоминания созданы:\n"
            f"Текст: {text}\n"
            f"С {local_start.strftime('%d.%m.%Y %H:%M')} до {local_end.strftime('%d.%m.%Y %H:%M')}\n"
            f"Каждые {frequency} минут"
        )
        await state.clear()
        
    except ValueError:
        await message.answer("Пожалуйста, введите целое число для частоты напоминаний:")

@dp.message(Command("list_reminds"))
async def list_reminds(message: Message):
    db = ReminderDatabase()
    db.connect()
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(
                "SELECT chat_id, text, time FROM reminders WHERE chat_id = %s ORDER BY time",
                (message.from_user.id,)
            )
            reminders = cursor.fetchall()
            
            if not reminders:
                await message.answer("У вас пока нет напоминаний")
                return
                
            response = "Ваши напоминания:\n\n"
            for i, reminder in enumerate(reminders, 1):
                chat_id, text, time_value = reminder
                # Check if time_value is already a datetime object
                if isinstance(time_value, datetime):
                    utc_time = time_value.replace(tzinfo=timezone.utc)
                else:
                    # If it's a string, parse it
                    utc_time = datetime.strptime(time_value, "%Y-%m-%d %H:%M:%S+00").replace(tzinfo=timezone.utc)
                
                local_time = utc_time.astimezone(LOCAL_TIMEZONE)
                response += f"{i}. {text}\nВремя: {local_time.strftime('%d.%m.%Y %H:%M')}\n\n"
            
            await message.answer(response)
    except Exception as e:
        logger.error(f"Error listing reminders: {e}")
        await message.answer("Произошла ошибка при получении списка напоминаний")
    finally:
        db.close()

async def main():
    try:
        # Start Kafka consumer in a separate task
        asyncio.create_task(asyncio.to_thread(start_kafka_consumer))
        logger.info("Kafka consumer started")
        
        # Start reminder sender
        start_reminder_sender()
        
        # Start the bot
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
