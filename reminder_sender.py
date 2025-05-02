import asyncio
from aiogram import Bot
from config import BOT_TOKEN
from database import ReminderDatabase
import logging
from datetime import datetime, timedelta, timezone
import schedule
import time
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set your local timezone (GMT+3)
LOCAL_TIMEZONE = timezone(timedelta(hours=3))

bot = Bot(BOT_TOKEN)

async def send_reminder(chat_id: int, text: str):
    try:
        await bot.send_message(chat_id=chat_id, text=f"🔔 Напоминание: {text}")
        logger.info(f"Successfully sent reminder to chat_id {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send reminder to chat_id {chat_id}: {e}")

def check_and_send_reminders():
    logger.info("Checking for reminders...")
    db = ReminderDatabase()
    db.connect()
    try:
        # Get current time in UTC
        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        one_minute_ago = current_time - timedelta(minutes=1)
        
        logger.info(f"Current time (UTC): {current_time}")
        logger.info(f"Checking for reminders between {one_minute_ago} and {current_time}")
        
        # Get reminders that should be sent now
        with db.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT r.id, r.chat_id, r.text, r.remind_id, rs.end_time 
                FROM reminders r
                JOIN reminder_series rs ON r.remind_id = rs.remind_id
                WHERE r.time <= %s AND r.time > %s
                ORDER BY r.time
                """,
                (current_time.strftime("%Y-%m-%d %H:%M:%S+00"), 
                 one_minute_ago.strftime("%Y-%m-%d %H:%M:%S+00"))
            )
            reminders = cursor.fetchall()
            
            if not reminders:
                logger.info("No reminders to send")
                return
                
            logger.info(f"Found {len(reminders)} reminders to send")
            
            # Create new event loop for this thread if it doesn't exist
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            for reminder in reminders:
                reminder_id, chat_id, text, remind_id, end_time = reminder
                logger.info(f"Processing reminder: id={reminder_id}, chat_id={chat_id}, text={text}, remind_id={remind_id}")
                
                try:
                    # Run the coroutine in the event loop
                    loop.run_until_complete(send_reminder(chat_id, text))
                    
                    # Delete this specific reminder
                    cursor.execute(
                        "DELETE FROM reminders WHERE id = %s",
                        (reminder_id,)
                    )
                    db.conn.commit()
                    logger.info(f"Deleted reminder with id {reminder_id}")
                    
                    # If this is the last reminder in the series (current time >= end_time)
                    if current_time >= end_time:
                        db.delete_reminder_series(remind_id)
                        logger.info(f"Deleted reminder series with remind_id {remind_id}")
                            
                except Exception as e:
                    logger.error(f"Error processing reminder {reminder_id}: {e}")
                    continue
                
    except Exception as e:
        logger.error(f"Error in check_and_send_reminders: {e}")
    finally:
        db.close()

def run_scheduler():
    logger.info("Starting scheduler...")
    # Check for reminders every minute
    schedule.every(1).minutes.do(check_and_send_reminders)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}")

def start_reminder_sender():
    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    logger.info("Reminder sender started") 