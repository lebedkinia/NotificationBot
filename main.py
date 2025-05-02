import asyncio
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    MenuButtonCommands,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from config import BOT_TOKEN
from database import ReminderDatabase
from kafka_handler import KafkaNotificationHandler, start_kafka_consumer
from kafka.errors import NoBrokersAvailable
from reminder_sender import start_reminder_sender
from user_settings import UserSettings
import logging
from datetime import datetime, timedelta, timezone
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Set your local timezone (GMT+3)
LOCAL_TIMEZONE = timezone(timedelta(hours=3))

bot = Bot(BOT_TOKEN)
dp = Dispatcher()
user_settings = UserSettings()

# Initialize Kafka handler
try:
    kafka_handler = KafkaNotificationHandler()
    logger.info("Kafka handler initialized successfully")
except NoBrokersAvailable as e:
    logger.error(f"Failed to initialize Kafka handler: {e}")
    raise


def kafka_clear():
    from kafka.admin import KafkaAdminClient

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

    топики = ["notifications"]
    admin_client.delete_topics(топики)


class ReminderStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_start_time = State()
    waiting_for_end_time = State()
    waiting_for_frequency = State()
    waiting_for_edit_text = State()
    waiting_for_edit_start = State()
    waiting_for_edit_end = State()
    waiting_for_edit_frequency = State()
    waiting_for_timezone = State()


def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="📝 Создать напоминание"),
                KeyboardButton(text="📋 Список напоминаний"),
            ],
            [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
    )


def get_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🕒 Изменить часовой пояс", callback_data="change_timezone"
                )
            ]
        ]
    )


def parse_date_time(date_str: str, user_id: int) -> tuple[datetime, bool]:
    """Parse date string with special rules and return (datetime, is_special_period)"""
    local_tz = user_settings.get_timezone(user_id)
    now = datetime.now(local_tz)

    # Special cases
    if date_str.lower() == "n":
        return now.astimezone(timezone.utc), False

    if date_str.lower() == "d":
        end_of_day = now.replace(hour=23, minute=59, second=59)
        return end_of_day.astimezone(timezone.utc), True

    if date_str.lower() == "w":
        days_until_end_of_week = 6 - now.weekday()
        end_of_week = now + timedelta(days=days_until_end_of_week)
        end_of_week = end_of_week.replace(hour=23, minute=59, second=59)
        return end_of_week.astimezone(timezone.utc), True

    if date_str.lower() == "m":
        if now.month == 12:
            end_of_month = now.replace(year=now.year + 1, month=1, day=1) - timedelta(
                days=1
            )
        else:
            end_of_month = now.replace(month=now.month + 1, day=1) - timedelta(days=1)
        end_of_month = end_of_month.replace(hour=23, minute=59, second=59)
        return end_of_month.astimezone(timezone.utc), True

    # Regular date parsing
    try:
        # Try parsing with time
        if " " in date_str:
            date_part, time_part = date_str.split(" ")
            if ":" in time_part:
                hour, minute = map(int, time_part.split(":"))
            else:
                hour, minute = 0, 0
        else:
            date_part = date_str
            hour, minute = 0, 0

        # Parse date part
        if "." in date_part:
            parts = date_part.split(".")
            if len(parts) == 2:  # DD.MM
                day, month = map(int, parts)
                year = now.year
                if (month < now.month) or (month == now.month and day < now.day):
                    year += 1
            elif len(parts) == 3:  # DD.MM.YYYY
                day, month, year = map(int, parts)
            else:
                raise ValueError("Invalid date format")
        else:  # Just day
            day = int(date_part)
            month = now.month
            year = now.year
            if day < now.day:
                month += 1
                if month > 12:
                    month = 1
                    year += 1

        # Create datetime in local timezone
        local_dt = datetime(year, month, day, hour, minute, tzinfo=local_tz)
        # Convert to UTC for storage
        utc_dt = local_dt.astimezone(timezone.utc)
        return utc_dt, False
    except Exception as e:
        logger.error(f"Error parsing date: {e}")
        raise ValueError("Неверный формат даты")


async def setup_bot_commands():
    await bot.set_my_commands(
        [
            types.BotCommand(command="start", description="Начать работу с ботом"),
            types.BotCommand(command="help", description="Показать справку"),
            types.BotCommand(
                command="create_remind", description="Создать новое напоминание"
            ),
            types.BotCommand(
                command="list_reminds", description="Показать список напоминаний"
            ),
            types.BotCommand(command="settings", description="Настройки бота"),
        ]
    )


def get_reminder_keyboard(remind_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить текст", callback_data=f"edit_text_{remind_id}"
                ),
                InlineKeyboardButton(
                    text="⏰ Изменить время", callback_data=f"edit_time_{remind_id}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Изменить частоту",
                    callback_data=f"edit_frequency_{remind_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Удалить", callback_data=f"delete_{remind_id}"
                ),
            ],
        ]
    )


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer(
        "Привет! Я бот для напоминаний. Используйте кнопки ниже для управления напоминаниями.",
        reply_markup=get_main_keyboard(),
    )


@dp.message(F.text == "❓ Помощь")
@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "Доступные команды:\n"
        "📝 Создать напоминание - создать новое напоминание\n"
        "📋 Список напоминаний - показать список напоминаний\n"
        "⚙️ Настройки - настройки бота\n"
        "❓ Помощь - показать это сообщение\n\n"
        "Специальные команды для даты:\n"
        "n - сейчас\n"
        "d - до конца дня\n"
        "w - до конца недели\n"
        "m - до конца месяца\n"
        "DD.MM - дата в текущем/следующем году\n"
        "DD - день в текущем/следующем месяце"
    )


@dp.message(F.text == "⚙️ Настройки")
@dp.message(Command("settings"))
async def settings(message: Message):
    await message.answer("Настройки бота:", reply_markup=get_settings_keyboard())


@dp.callback_query(F.data == "change_timezone")
async def change_timezone(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "Введите смещение часового пояса в часах (например, 3 для GMT+3):"
    )
    await state.set_state(ReminderStates.waiting_for_timezone)
    await callback.answer()


@dp.message(ReminderStates.waiting_for_timezone)
async def process_timezone(message: Message, state: FSMContext):
    try:
        offset = int(message.text)
        if not -12 <= offset <= 14:
            await message.answer(
                "Смещение должно быть от -12 до +14 часов. Попробуйте снова:"
            )
            return

        user_settings.set_timezone(message.from_user.id, offset)
        await message.answer(f"Часовой пояс установлен на GMT{offset:+d}")
    except ValueError:
        await message.answer(
            "Пожалуйста, введите целое число для смещения часового пояса"
        )
    await state.clear()


@dp.message(F.text == "📝 Создать напоминание")
@dp.message(Command("create_remind"))
async def create_remind(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, введите текст напоминания:")
    await state.set_state(ReminderStates.waiting_for_text)


@dp.message(ReminderStates.waiting_for_text)
async def process_text(message: Message, state: FSMContext):
    await state.update_data(text=message.text)
    await message.answer(
        "Теперь введите время начала напоминаний.\n"
        "Вы можете использовать:\n"
        "n - сейчас\n"
        "d - до конца дня\n"
        "w - до конца недели\n"
        "m - до конца месяца\n"
        "DD.MM - дата в текущем/следующем году\n"
        "DD - день в текущем/следующем месяце"
    )
    await state.set_state(ReminderStates.waiting_for_start_time)


@dp.message(ReminderStates.waiting_for_start_time)
async def process_start_time(message: Message, state: FSMContext):
    try:
        start_time, is_special_period = parse_date_time(
            message.text, message.from_user.id
        )
        await state.update_data(start_time=start_time)

        if is_special_period:
            # Skip end time for special periods
            await state.set_state(ReminderStates.waiting_for_frequency)
            await message.answer("Введите частоту напоминаний в минутах (целое число):")
        else:
            await message.answer(
                "Теперь введите время окончания напоминаний в том же формате:"
            )
            await state.set_state(ReminderStates.waiting_for_end_time)
    except ValueError as e:
        await message.answer(str(e))


@dp.message(ReminderStates.waiting_for_end_time)
async def process_end_time(message: Message, state: FSMContext):
    try:
        end_time, _ = parse_date_time(message.text, message.from_user.id)
        data = await state.get_data()
        start_time = data["start_time"]

        if end_time <= start_time:
            await message.answer(
                "Время окончания должно быть позже времени начала. Попробуйте снова:"
            )
            return

        await state.update_data(end_time=end_time)
        await message.answer("Введите частоту напоминаний в минутах (целое число):")
        await state.set_state(ReminderStates.waiting_for_frequency)
    except ValueError as e:
        await message.answer(str(e))


@dp.message(ReminderStates.waiting_for_frequency)
async def process_frequency(message: Message, state: FSMContext):
    try:
        frequency = int(message.text)
        if frequency <= 0:
            await message.answer(
                "Частота должна быть положительным числом. Попробуйте снова:"
            )
            return

        data = await state.get_data()
        start_time = data["start_time"]
        end_time = data["end_time"]
        text = data["text"]

        # Generate a single remind_id for all reminders in this series
        remind_id = int(datetime.utcnow().timestamp())

        # Store reminder series information
        db = ReminderDatabase()
        db.connect()
        try:
            db.add_reminder_series(
                chat_id=message.from_user.id,
                remind_id=remind_id,
                text=text,
                end_time=end_time.strftime("%Y-%m-%d %H:%M:%S+00")
            )
        except Exception as e:
            logger.error(f"Error storing reminder series: {e}")
            await message.answer("Произошла ошибка при создании напоминаний")
            return
        finally:
            db.close()

        # Generate reminders and send to Kafka
        current_time = start_time
        reminders_count = 0

        while current_time <= end_time:
            notification_data = {
                "chat_id": message.from_user.id,
                "remind_id": remind_id,
                "text": text,
                "time": current_time.strftime("%Y-%m-%d %H:%M:%S+00"),
            }
            kafka_handler.send_notification(notification_data)
            reminders_count += 1
            current_time += timedelta(minutes=frequency)

        # Convert times back to local timezone for display
        local_start = start_time.astimezone(LOCAL_TIMEZONE)
        local_end = end_time.astimezone(LOCAL_TIMEZONE)

        await message.answer(
            f"Отлично! Напоминания созданы:\n"
            f"Текст: {text}\n"
            f"С {local_start.strftime('%d.%m.%Y %H:%M')} до {local_end.strftime('%d.%m.%Y %H:%M')}\n"
            f"Каждые {frequency} минут\n"
            f"Всего напоминаний: {reminders_count}"
        )
        await state.clear()

    except ValueError:
        await message.answer("Пожалуйста, введите целое число для частоты напоминаний:")


@dp.message(F.text == "📋 Список напоминаний")
@dp.message(Command("list_reminds"))
async def list_reminds(message: Message):
    db = ReminderDatabase()
    db.connect()
    try:
        with db.conn.cursor() as cursor:
            # Get unique remind_ids for this user
            cursor.execute(
                """
                SELECT DISTINCT remind_id, text, MIN(time) as first_time, MAX(time) as last_time 
                FROM reminders 
                WHERE chat_id = %s 
                GROUP BY remind_id, text 
                ORDER BY first_time
                """,
                (message.from_user.id,),
            )
            reminder_series = cursor.fetchall()

            if not reminder_series:
                await message.answer("У вас пока нет напоминаний")
                return

            for series in reminder_series:
                remind_id, text, first_time, last_time = series

                # Convert times from UTC to local timezone
                if isinstance(first_time, str):
                    first_time = datetime.strptime(
                        first_time, "%Y-%m-%d %H:%M:%S+00"
                    ).replace(tzinfo=timezone.utc)
                if isinstance(last_time, str):
                    last_time = datetime.strptime(
                        last_time, "%Y-%m-%d %H:%M:%S+00"
                    ).replace(tzinfo=timezone.utc)

                local_tz = user_settings.get_timezone(message.from_user.id)
                first_time = first_time.astimezone(local_tz)
                last_time = last_time.astimezone(local_tz)

                response = (
                    f"Текст: {text}\n"
                    f"*{first_time.strftime('%d.%m.%Y %H:%M')}* — "
                    f"*{last_time.strftime('%d.%m.%Y %H:%M')}*"
                )

                await message.answer(
                    response,
                    reply_markup=get_reminder_keyboard(remind_id),
                    parse_mode="Markdown",
                )
    except Exception as e:
        logger.error(f"Error listing reminders: {e}")
        await message.answer("Произошла ошибка при получении списка напоминаний")
    finally:
        db.close()


@dp.callback_query(F.data.startswith("edit_text_"))
async def edit_text(callback: types.CallbackQuery, state: FSMContext):
    remind_id = int(callback.data.split("_")[-1])
    await state.update_data(editing_remind_id=remind_id)
    await callback.message.answer("Введите новый текст напоминания:")
    await state.set_state(ReminderStates.waiting_for_edit_text)
    await callback.answer()


@dp.callback_query(F.data.startswith("edit_time_"))
async def edit_time(callback: types.CallbackQuery, state: FSMContext):
    remind_id = int(callback.data.split("_")[-1])
    await state.update_data(editing_remind_id=remind_id)
    await callback.message.answer(
        "Введите новое время начала в формате ДД.ММ.ГГГГ ЧЧ:ММ\n"
        "Например: 01.01.2024 12:00"
    )
    await state.set_state(ReminderStates.waiting_for_edit_start)
    await callback.answer()


@dp.callback_query(F.data.startswith("edit_frequency_"))
async def edit_frequency(callback: types.CallbackQuery, state: FSMContext):
    remind_id = int(callback.data.split("_")[-1])
    await state.update_data(editing_remind_id=remind_id)
    await callback.message.answer("Введите новую частоту напоминаний в минутах:")
    await state.set_state(ReminderStates.waiting_for_edit_frequency)
    await callback.answer()


@dp.callback_query(F.data.startswith("delete_"))
async def delete_reminder(callback: types.CallbackQuery):
    remind_id = int(callback.data.split("_")[-1])
    db = ReminderDatabase()
    db.connect()
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM reminders WHERE remind_id = %s AND chat_id = %s",
                (remind_id, callback.from_user.id),
            )
            db.conn.commit()
            await callback.message.edit_text(
                f"{callback.message.text}\n\n❌ Напоминание удалено"
            )
    except Exception as e:
        logger.error(f"Error deleting reminder: {e}")
        await callback.answer("Произошла ошибка при удалении напоминания")
    finally:
        db.close()
    await callback.answer()


@dp.message(ReminderStates.waiting_for_edit_text)
async def process_edit_text(message: Message, state: FSMContext):
    data = await state.get_data()
    remind_id = data["editing_remind_id"]
    new_text = message.text

    db = ReminderDatabase()
    db.connect()
    try:
        with db.conn.cursor() as cursor:
            cursor.execute(
                "UPDATE reminders SET text = %s WHERE remind_id = %s AND chat_id = %s",
                (new_text, remind_id, message.from_user.id),
            )
            db.conn.commit()
            await message.answer("Текст напоминания обновлен")
    except Exception as e:
        logger.error(f"Error updating reminder text: {e}")
        await message.answer("Произошла ошибка при обновлении текста")
    finally:
        db.close()
    await state.clear()


@dp.message(ReminderStates.waiting_for_edit_start)
async def process_edit_start(message: Message, state: FSMContext):
    try:
        local_time = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        local_time = local_time.replace(tzinfo=LOCAL_TIMEZONE)
        new_start_time = local_time.astimezone(timezone.utc)

        data = await state.get_data()
        remind_id = data["editing_remind_id"]

        db = ReminderDatabase()
        db.connect()
        try:
            with db.conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE reminders SET time = %s WHERE remind_id = %s AND chat_id = %s",
                    (
                        new_start_time.strftime("%Y-%m-%d %H:%M:%S+00"),
                        remind_id,
                        message.from_user.id,
                    ),
                )
                db.conn.commit()
                await message.answer("Время начала напоминания обновлено")
        except Exception as e:
            logger.error(f"Error updating reminder time: {e}")
            await message.answer("Произошла ошибка при обновлении времени")
        finally:
            db.close()
    except ValueError:
        await message.answer(
            "Неверный формат даты. Пожалуйста, используйте формат ДД.ММ.ГГГГ ЧЧ:ММ\n"
            "Например: 01.01.2024 12:00"
        )
    await state.clear()


@dp.message(ReminderStates.waiting_for_edit_frequency)
async def process_edit_frequency(message: Message, state: FSMContext):
    try:
        new_frequency = int(message.text)
        if new_frequency <= 0:
            await message.answer(
                "Частота должна быть положительным числом. Попробуйте снова:"
            )
            return

        data = await state.get_data()
        remind_id = data["editing_remind_id"]

        db = ReminderDatabase()
        db.connect()
        try:
            with db.conn.cursor() as cursor:
                # Get current reminder data
                cursor.execute(
                    "SELECT time FROM reminders WHERE remind_id = %s AND chat_id = %s ORDER BY time",
                    (remind_id, message.from_user.id),
                )
                times = cursor.fetchall()

                if not times:
                    await message.answer("Напоминание не найдено")
                    return

                start_time = datetime.strptime(
                    times[0][0], "%Y-%m-%d %H:%M:%S+00"
                ).replace(tzinfo=timezone.utc)
                end_time = datetime.strptime(
                    times[-1][0], "%Y-%m-%d %H:%M:%S+00"
                ).replace(tzinfo=timezone.utc)

                # Delete old reminders
                cursor.execute(
                    "DELETE FROM reminders WHERE remind_id = %s AND chat_id = %s",
                    (remind_id, message.from_user.id),
                )

                # Create new reminders with new frequency
                current_time = start_time
                while current_time <= end_time:
                    cursor.execute(
                        """
                        INSERT INTO reminders (chat_id, remind_id, text, time)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            message.from_user.id,
                            remind_id,
                            data.get("text", ""),
                            current_time.strftime("%Y-%m-%d %H:%M:%S+00"),
                        ),
                    )
                    current_time += timedelta(minutes=new_frequency)

                db.conn.commit()
                await message.answer("Частота напоминаний обновлена")
        except Exception as e:
            logger.error(f"Error updating reminder frequency: {e}")
            await message.answer("Произошла ошибка при обновлении частоты")
        finally:
            db.close()
    except ValueError:
        await message.answer("Пожалуйста, введите целое число для частоты напоминаний:")
    await state.clear()


async def main():
    try:
        # Set up bot commands
        await setup_bot_commands()

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
