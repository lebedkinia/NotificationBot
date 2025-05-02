from database import ReminderDatabase
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)

class UserSettings:
    def __init__(self):
        self.db = ReminderDatabase()
        self.db.connect()
        self._create_settings_table()

    def _create_settings_table(self):
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_settings (
                        user_id BIGINT PRIMARY KEY,
                        timezone_offset INTEGER DEFAULT 3
                    )
                """)
                self.db.conn.commit()
        except Exception as e:
            logger.error(f"Error creating settings table: {e}")

    def get_timezone(self, user_id: int) -> timezone:
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT timezone_offset FROM user_settings WHERE user_id = %s",
                    (user_id,)
                )
                result = cursor.fetchone()
                if result:
                    return timezone(timedelta(hours=result[0]))
                # Default to GMT+3 if no settings found
                return timezone(timedelta(hours=3))
        except Exception as e:
            logger.error(f"Error getting timezone: {e}")
            return timezone(timedelta(hours=3))

    def set_timezone(self, user_id: int, offset: int):
        try:
            with self.db.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO user_settings (user_id, timezone_offset)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET timezone_offset = EXCLUDED.timezone_offset
                """, (user_id, offset))
                self.db.conn.commit()
        except Exception as e:
            logger.error(f"Error setting timezone: {e}")

    def close(self):
        self.db.close() 