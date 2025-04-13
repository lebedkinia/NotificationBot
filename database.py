import psycopg2
from psycopg2 import sql


class ReminderDatabase:
    def __init__(
        self,
        host="127.0.0.1",
        user="reminder_user",
        password="reminder_pass",
        database="reminders",
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print("Connected to the database.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            print("Connection closed.")

    def create_table(self):
        try:
            with self.conn.cursor() as cursor:
                create_table_query = sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS reminders (
                        id SERIAL PRIMARY KEY,
                        chat_id BIGINT NOT NULL,
                        remind_id BIGINT NOT NULL,
                        text TEXT NOT NULL,
                        time TIMESTAMP WITH TIME ZONE NOT NULL
                    )
                """
                )
                cursor.execute(create_table_query)
                self.conn.commit()
                print("Table 'reminders' created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def add_reminder(self, chat_id: int, remind_id: int, text: str, time: str):
        try:
            with self.conn.cursor() as cursor:
                insert_query = sql.SQL(
                    """
                    INSERT INTO reminders (chat_id, remind_id, text, time)
                    VALUES (%s, %s, %s, %s)
                """
                )
                cursor.execute(insert_query, (chat_id, remind_id, text, time))
                self.conn.commit()
                print("Reminder added successfully.")
        except Exception as e:
            print(f"Error adding reminder: {e}")

    def delete_reminder(self, remind_id: int):
        try:
            with self.conn.cursor() as cursor:
                delete_query = sql.SQL(
                    """
                    DELETE FROM reminders WHERE remind_id = %s
                """
                )
                cursor.execute(delete_query, (remind_id,))
                self.conn.commit()
                print("Reminder deleted successfully.")
        except Exception as e:
            print(f"Error deleting reminder: {e}")


if __name__ == "__main__":
    db = ReminderDatabase()

    db.connect()
    db.create_table()

    # db.add_reminder(
    #     chat_id=12345,
    #     remind_id=67890,
    #     text="Test reminder",
    #     time="2023-10-01 12:00:00+00",
    # )

    db.delete_reminder(remind_id=67890)

    db.close()
